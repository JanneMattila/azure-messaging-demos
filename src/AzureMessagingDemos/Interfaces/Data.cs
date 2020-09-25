using System;
using System.Text.Json.Serialization;

namespace AzureMessagingDemos.Interfaces
{
    public class Data
    {
        [JsonPropertyName("id")]
        public string ID { get; set; }

        [JsonPropertyName("date")]
        public DateTime Date { get; set; }
    }
}
