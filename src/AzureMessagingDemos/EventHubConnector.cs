using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace AzureMessagingDemos;

public class EventHubConnector
{
    private readonly EventHubProducerClient _sender;
    private readonly EventProcessorClient _receiver;
    private DateTime _messageLastReceived = DateTime.UtcNow;

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

    private Task ProcessEventHandler(ProcessEventArgs eventArgs)
    {
        var messageReceived = DateTime.UtcNow;
        var payloadJson = Encoding.UTF8.GetString(eventArgs.Data.EventBody.ToArray());
        var data = JsonSerializer.Deserialize<Interfaces.Data>(payloadJson);
        Console.WriteLine($"ID: {data.ID} - E2E: {(DateTime.UtcNow - data.Date).TotalMilliseconds} ms - processing delay: {(messageReceived - _messageLastReceived).TotalMilliseconds} ms");
        _messageLastReceived = messageReceived;
        return Task.CompletedTask;
    }

    private Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
    {
        // Write details about the error to the console window
        Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
        Console.WriteLine(eventArgs.Exception.Message);
        return Task.CompletedTask;
    }
}
