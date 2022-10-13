using Azure.Messaging.ServiceBus;
using AzureMessagingDemos.Interfaces;
using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace AzureMessagingDemos;

public class ServiceBusConnector
{
    private readonly ServiceBusSender _sender;
    private readonly ServiceBusReceiver _receiver;

    public ServiceBusConnector(string sendConnectionString, string listenConnectionString, string queueName)
    {
        var clientSender = new ServiceBusClient(sendConnectionString, new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpWebSockets
        });
        _sender = clientSender.CreateSender(queueName);

        var clientReceiver = new ServiceBusClient(listenConnectionString, new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpWebSockets
        });
        _receiver = clientReceiver.CreateReceiver(queueName);
    }

    public async Task SendAsync(int id)
    {
        var data = new Data
        {
            ID = id.ToString(),
            Date = DateTime.UtcNow
        };
        var payloadJson = JsonSerializer.Serialize(data);
        var payload = Encoding.UTF8.GetBytes(payloadJson);
        var request = new ServiceBusMessage(payload);

        await _sender.SendMessageAsync(request).ConfigureAwait(false);

        Console.Write("+");
    }

    public async Task ReceiveAsync(int timeout)
    {
        var messageLastReceived = DateTime.UtcNow;

        while (true)
        {
            var message = await _receiver.ReceiveMessageAsync().ConfigureAwait(false);
            if (message != null)
            {
                var processingDelay = messageLastReceived;
                var messageReceived = DateTime.UtcNow;
                if (timeout != 0)
                {
                    await Task.Delay(timeout);
                }

                await _receiver.CompleteMessageAsync(message).ConfigureAwait(false);
                var payloadJson = Encoding.UTF8.GetString(message.Body);
                var data = JsonSerializer.Deserialize<Data>(payloadJson);
                Console.WriteLine($"ID: {data.ID} - E2E: {(DateTime.UtcNow - data.Date).TotalMilliseconds} ms - processing delay: {(messageReceived - messageLastReceived).TotalMilliseconds} ms");
                messageLastReceived = messageReceived;
            }
            else
            {
                Console.Write("-");
            }
        }
    }
}
