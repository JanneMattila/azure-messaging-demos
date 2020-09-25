using AzureMessagingDemos.Interfaces;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace AzureMessagingDemos
{
    public class ServiceBusConnector
    {
        private readonly string _sendConnectionString;
        private readonly string _listenConnectionString;

        private readonly MessageSender _sender;

        public ServiceBusConnector(string sendConnectionString, string listenConnectionString)
        {
            _sendConnectionString = sendConnectionString;
            _listenConnectionString = listenConnectionString;

            var connectionStringBuilder = new ServiceBusConnectionStringBuilder(_sendConnectionString)
            {
                TransportType = TransportType.AmqpWebSockets
            };
            _sender = new MessageSender(connectionStringBuilder);
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
            var request = new Message(payload);

            await _sender.SendAsync(request).ConfigureAwait(false);

            Console.Write("+");
        }

        public async Task ReceiveAsync(int timeout)
        {
            var connectionStringBuilder = new ServiceBusConnectionStringBuilder(_listenConnectionString)
            {
                TransportType = TransportType.AmqpWebSockets
            };
            var receiver = new MessageReceiver(connectionStringBuilder);
            var messageLastReceived = DateTime.UtcNow;

            while (true)
            {
                var message = await receiver.ReceiveAsync().ConfigureAwait(false);
                if (message != null)
                {
                    var processingDelay = messageLastReceived;
                    var messageReceived = DateTime.UtcNow;
                    if (timeout != 0)
                    {
                        await Task.Delay(timeout);
                    }

                    await receiver.CompleteAsync(message.SystemProperties.LockToken);
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
}
