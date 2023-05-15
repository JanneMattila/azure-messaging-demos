using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using System;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace AzureMessagingDemos;

// Example from here:
// https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/eventhub/Azure.Messaging.EventHubs.Processor/samples/Sample01_HelloWorld.md
public class EventHubConnector
{
    private readonly EventHubProducerClient _sender;
    private readonly EventProcessorClient _receiver;
    private DateTime _messageLastReceived = DateTime.UtcNow;
    private ConcurrentDictionary<string, int> _partitionEventCount = new ConcurrentDictionary<string, int>();

    public EventHubConnector(string blobConnectionString, string blobContainerName, string sendConnectionString, string listenConnectionString, string hubName, string consumerGroup)
    {
        _sender = new EventHubProducerClient(sendConnectionString, hubName);

        var storageClient = new BlobContainerClient(blobConnectionString, blobContainerName);
        _receiver = new EventProcessorClient(storageClient, consumerGroup, listenConnectionString, hubName);
        _receiver.ProcessEventAsync += ProcessEventHandler;
        _receiver.ProcessErrorAsync += ProcessErrorHandler;
    }

    public async Task SendAsync(int id)
    {
        var data = new Interfaces.Data
        {
            ID = id.ToString(),
            Date = DateTime.UtcNow
        };
        var payloadJson = JsonSerializer.Serialize(data);
        var payload = Encoding.UTF8.GetBytes(payloadJson);

        using var eventBatch = await _sender.CreateBatchAsync();
        if (!eventBatch.TryAdd(new EventData(payload)))
        {
            throw new Exception($"Event is too large for the batch and cannot be sent.");
        }
        await _sender.SendAsync(eventBatch).ConfigureAwait(false);

        Console.Write("+");
    }

    public async Task ReceiveAsync(int timeout)
    {
        await _receiver.StartProcessingAsync().ConfigureAwait(false);

        await Task.Delay(TimeSpan.FromMilliseconds(timeout)).ConfigureAwait(false);

        await _receiver.StopProcessingAsync().ConfigureAwait(false);
    }

    private async Task ProcessEventHandler(ProcessEventArgs args)
    {
        try
        {
            if (args.CancellationToken.IsCancellationRequested)
            {
                return;
            }

            var messageReceived = DateTime.UtcNow;
            var payloadJson = Encoding.UTF8.GetString(args.Data.EventBody.ToArray());
            var data = JsonSerializer.Deserialize<Interfaces.Data>(payloadJson);
            Console.WriteLine($"ID: {data.ID} - E2E: {(DateTime.UtcNow - data.Date).TotalMilliseconds} ms - processing delay: {(messageReceived - _messageLastReceived).TotalMilliseconds} ms");
            _messageLastReceived = messageReceived;

            var partition = args.Partition.PartitionId;
            var eventsSinceLastCheckpoint = _partitionEventCount.AddOrUpdate(
                key: partition,
                addValue: 1,
                updateValueFactory: (_, currentCount) => currentCount + 1);

            if (eventsSinceLastCheckpoint >= 50)
            {
                await args.UpdateCheckpointAsync();
                _partitionEventCount[partition] = 0;
            }
        }
        catch (Exception)
        {
        }
    }

    private Task ProcessErrorHandler(ProcessErrorEventArgs args)
    {
        // Write details about the error to the console window
        Console.WriteLine($"\tPartition '{args.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
        Console.WriteLine(args.Exception.Message);
        return Task.CompletedTask;
    }
}
