using System.Text.Json.Serialization;

namespace AzureDigitalTwinsUpdaterFunc.Interfaces;

public class DigitalTwinUpdateRequest
{
    [JsonPropertyName("id")]
    public string ID { get; set; } = string.Empty;

    [JsonPropertyName("value")]
    public int Value { get; set; }
}
