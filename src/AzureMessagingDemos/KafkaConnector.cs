using AzureMessagingDemos.Interfaces;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace AzureMessagingDemos;

public class KafkaConnector
{
    private readonly IProducer<string, byte[]> _sender;
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly string _topic;

    public KafkaConnector(string kafkaServers, string kafkaConnectionString, string kafkaTopic)
    {
        var config = new Dictionary<string, string>
        {
            { "bootstrap.servers", kafkaServers },
            { "security.protocol", "SASL_SSL" },
            { "sasl.mechanism", "PLAIN" },
            { "sasl.username", "$ConnectionString" },
            { "sasl.password", kafkaConnectionString }
        };

        _topic = kafkaTopic;
        _sender = new ProducerBuilder<string, byte[]>(config).Build();

        var consumerConfig = new ConsumerConfig(new ClientConfig(config))
        {
            GroupId = "group1",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        _consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
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

    public async Task ReceiveAsync(int timeout)
    {
        var messageLastReceived = DateTime.UtcNow;

        _consumer.Subscribe(_topic);
        while (true)
        {
            var consumeResult = _consumer.Consume(15_000);
            if (consumeResult != null)
            {
                var messageReceived = DateTime.UtcNow;
                if (timeout != 0)
                {
                    await Task.Delay(timeout);
                }

                var payloadJson = Encoding.UTF8.GetString(consumeResult.Message.Value);
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
