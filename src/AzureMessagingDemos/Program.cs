using Microsoft.Extensions.Configuration;
using System;
using System.Threading.Tasks;

namespace AzureMessagingDemos;

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("Azure Messaging Demos");
        var builder = new ConfigurationBuilder()
            .AddUserSecrets<Program>()
            .AddJsonFile("appsettings.json");

        var configuration = builder.Build();

        if (string.Compare(args[0], "EventGrid", StringComparison.InvariantCultureIgnoreCase) == 0)
        {
            var endpoint = configuration.GetValue<string>("EventGrid:Endpoint");
            var key = configuration.GetValue<string>("EventGrid:Key");

            var connector = new EventGridConnector(endpoint, key);
            await connector.SendAsync(1);
        }
        else if (string.Compare(args[0], "EventHub", StringComparison.InvariantCultureIgnoreCase) == 0)
        {
            var eventHubBlobConnectionString = configuration.GetValue<string>("EventHub:BlobConnectionString");
            var eventHubBlobContainerName = configuration.GetValue<string>("EventHub:BlobContainerName");
            var eventHubSendConnectionString = configuration.GetValue<string>("EventHub:SendConnectionString");
            var eventHubListenConnectionString = configuration.GetValue<string>("EventHub:ListenConnectionString");
            var eventHubName = configuration.GetValue<string>("EventHub:Name");
            var eventHubConsumerGroup = configuration.GetValue<string>("EventHub:ConsumerGroup");

            var connector = new EventHubConnector(eventHubBlobConnectionString, eventHubBlobContainerName,
                eventHubSendConnectionString, eventHubListenConnectionString,
                eventHubName, eventHubConsumerGroup);
            //await connector.SendAsync(1);
            await connector.ReceiveAsync(60_000);
        }
        else if (string.Compare(args[0], "ServiceBus", StringComparison.InvariantCultureIgnoreCase) == 0)
        {
            var serviceBusSendConnectionString = configuration.GetValue<string>("ServiceBus:SendConnectionString");
            var serviceBusListenConnectionString = configuration.GetValue<string>("ServiceBus:ListenConnectionString");
            var serviceBusTopic = configuration.GetValue<string>("ServiceBus:Topic");

            var connector = new ServiceBusConnector(serviceBusSendConnectionString, serviceBusListenConnectionString, serviceBusTopic);
            await connector.SendAsync(1);
            await connector.ReceiveAsync(0);
        }
        else if (string.Compare(args[0], "Kafka", StringComparison.InvariantCultureIgnoreCase) == 0)
        {
            var kafkaServers = configuration.GetValue<string>("Kafka:Servers");
            var kafkaConnectionString = configuration.GetValue<string>("Kafka:ConnectionString");
            var kafkaTopic = configuration.GetValue<string>("Kafka:Topic");

            var connector = new KafkaConnector(kafkaServers, kafkaConnectionString, kafkaTopic);
            await connector.SendAsync(1);
            await connector.ReceiveAsync(0);
        }
        else if (string.Compare(args[0], "MQTT", StringComparison.InvariantCultureIgnoreCase) == 0)
        {
            var mqttServer = configuration.GetValue<string>("MQTT:Server");
            var mqttUsername = configuration.GetValue<string>("MQTT:Username");
            var mqttPassword = configuration.GetValue<string>("MQTT:Password");
            var mqttTopic = configuration.GetValue<string>("MQTT:Topic");

            var connector = new MQTTConnector(mqttServer, mqttUsername, mqttPassword, mqttTopic);
            await connector.SendAsync(1);
            await connector.ReceiveAsync(0);
        }

        Console.WriteLine("Done!");
    }
}
