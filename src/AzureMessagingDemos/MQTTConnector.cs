using MQTTnet;
using MQTTnet.Client;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace AzureMessagingDemos;

public class MQTTConnector
{
    private readonly MqttFactory _mqttFactory;
    private readonly IMqttClient _mqttClient;
    private readonly MqttClientOptions _mqttClientOptions;
    private readonly string _topicName;

    private bool _disposedValue;

    public MQTTConnector(string server, string username, string password, string topicName)
    {
        _topicName = topicName;

        _mqttFactory = new MqttFactory();
        _mqttClient = _mqttFactory.CreateMqttClient();
        _mqttClientOptions = new MqttClientOptionsBuilder()
            .WithTcpServer(server)
            .WithCredentials(username, password)
            .Build();
    }

    public async Task SendAsync(int id)
    {
        var data = new Interfaces.Data
        {
            ID = id.ToString(),
            Date = DateTime.UtcNow
        };
        var payloadJson = JsonSerializer.Serialize(data);

        var applicationMessage = new MqttApplicationMessageBuilder()
            .WithTopic(_topicName)
            .WithPayload(payloadJson)
            .WithRetainFlag()
            .Build();

        await _mqttClient.ConnectAsync(_mqttClientOptions, CancellationToken.None).ConfigureAwait(false);
        await _mqttClient.PublishAsync(applicationMessage, CancellationToken.None).ConfigureAwait(false);

        Console.Write("+");
    }

    public async Task ReceiveAsync(int timeout)
    {
        var messageLastReceived = DateTime.UtcNow;
        _mqttClient.ApplicationMessageReceivedAsync += async e =>
        {
            var processingDelay = messageLastReceived;
            var messageReceived = DateTime.UtcNow;
            if (timeout != 0)
            {
                await Task.Delay(timeout);
            }

            var payloadJson = e.ApplicationMessage.ConvertPayloadToString();
            var data = JsonSerializer.Deserialize<Interfaces.Data>(payloadJson);
            Console.WriteLine($"ID: {data.ID} - E2E: {(DateTime.UtcNow - data.Date).TotalMilliseconds} ms - processing delay: {(messageReceived - messageLastReceived).TotalMilliseconds} ms");
            messageLastReceived = messageReceived;
        };

        var mqttSubscribeOptions = _mqttFactory.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(
                f =>
                {
                    f.WithTopic(_topicName);
                })
            .Build();

        var result = await _mqttClient.SubscribeAsync(mqttSubscribeOptions, CancellationToken.None);
        await Task.Delay(TimeSpan.FromDays(1));
    }
}
