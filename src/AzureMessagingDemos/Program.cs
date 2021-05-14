using Microsoft.Extensions.Configuration;
using System;
using System.Threading.Tasks;

namespace AzureMessagingDemos
{
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
            else if (string.Compare(args[0], "ServiceBus", StringComparison.InvariantCultureIgnoreCase) == 0)
            {
                var serviceBusSendConnectionString = configuration.GetValue<string>("ServiceBus:SendConnectionString");
                var serviceBusListenConnectionString = configuration.GetValue<string>("ServiceBus:ListenConnectionString");

                var connector = new ServiceBusConnector(serviceBusSendConnectionString, serviceBusListenConnectionString);
                await connector.SendAsync(1);
                await connector.ReceiveAsync(5_000);
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

            Console.WriteLine("Done!");
        }
    }
}
