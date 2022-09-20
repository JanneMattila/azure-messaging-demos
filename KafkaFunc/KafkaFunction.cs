using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Kafka;
using Microsoft.Extensions.Logging;

namespace KafkaFunc
{
    public class KafkaFunction
    {
        // KafkaTrigger sample
        // Consume the message from "cars" on the LocalBroker.
        // Add `BrokerList` and `Password` to the local.settings.json
        // For EventHubs
        // "BrokerList": "{EVENT_HUBS_NAMESPACE}.servicebus.windows.net:9093"
        // "Password":"{EVENT_HUBS_CONNECTION_STRING}
        [FunctionName("KafkaFunction")]
        public static void Run(
            [KafkaTrigger("BrokerList",
                          "cars",
                          Username = "$ConnectionString",
                          Password = "%Password%",
                          Protocol = BrokerProtocol.SaslSsl,
                          AuthenticationMode = BrokerAuthenticationMode.Plain,
                          ConsumerGroup = "$Default")] KafkaEventData<string>[] events, ILogger log)
        {
            foreach (KafkaEventData<string> eventData in events)
            {
                log.LogInformation($"C# Kafka trigger function processed a message: {eventData.Value}");
            }
        }
    }
}
