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
            if (string.Compare(args[0], "ServiceBus", StringComparison.InvariantCultureIgnoreCase) == 0)
            {
                var serviceBusSendConnectionString = configuration.GetValue<string>("ServiceBusSendConnectionString");
                var serviceBusListenConnectionString = configuration.GetValue<string>("ServiceBusListenConnectionString");

                var connector = new ServiceBusConnector(serviceBusSendConnectionString, serviceBusListenConnectionString);
                await connector.SendAsync(1);
                await connector.ReceiveAsync(5_000);
            }

            Console.WriteLine("Done!");
        }
    }
}
