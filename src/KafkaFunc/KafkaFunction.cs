using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Kafka;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

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
        public static async Task Run(
            [KafkaTrigger("BrokerList",
                          "cars",
                          Username = "$ConnectionString",
                          Password = "%Password%",
                          //Protocol = BrokerProtocol.SaslSsl,
                          Protocol = BrokerProtocol.Plaintext, // Due to local test setup
                          AuthenticationMode = BrokerAuthenticationMode.Plain,
                          ConsumerGroup = "$Default")] KafkaEventData<byte[]>[] events, ILogger log)
        {
            var registry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
            {
                Url = "http://localhost:8081",
                BasicAuthUserInfo = "user:password"
            });
            var avroDeserializerWithRegistry = new AvroDeserializer<GenericRecord>(registry);

            foreach (var eventData in events)
            {
                var record = await avroDeserializerWithRegistry.DeserializeAsync(eventData.Value, false, SerializationContext.Empty);

                log.LogInformation($"Car: {record["year"]}");
            }
        }
    }
}
