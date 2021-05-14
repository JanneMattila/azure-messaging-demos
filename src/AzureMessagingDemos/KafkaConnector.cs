using AzureMessagingDemos.Interfaces;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace AzureMessagingDemos
{
    public class KafkaConnector
    {
        private readonly IProducer<string, byte[]> _sender;
        private readonly string _topic;

        public KafkaConnector(string kafkaServers, string kafkaConnectionString, string kafkaTopic)
        {
            var config = new List<KeyValuePair<string, string>>
            {
                new("bootstrap.servers", kafkaServers),
                new("request.timeout.ms", "60000"),
                new("security.protocol", "SASL_SSL"),
                new("sasl.mechanism", "PLAIN"),
                new("sasl.username", "$ConnectionString"),
                new("sasl.password", kafkaConnectionString)
            };

            _topic = kafkaTopic;
            _sender = new ProducerBuilder<string, byte[]>(config).Build();
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

            var deliveryResult = await _sender.ProduceAsync(_topic, new Message<string, byte[]> { Key = id.ToString(), Value = payload });
            Console.Write("+");
        }
    }
}
