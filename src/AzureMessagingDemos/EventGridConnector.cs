using Azure;
using Azure.Messaging.EventGrid;
using AzureMessagingDemos.Interfaces;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AzureMessagingDemos
{
    public class EventGridConnector
    {
        private readonly EventGridPublisherClient _sender;

        public EventGridConnector(string endpoint, string key)
        {
            _sender = new EventGridPublisherClient(
                new Uri(endpoint), new AzureKeyCredential(key));
        }

        public async Task SendAsync(int id)
        {
            var data = new Data
            {
                ID = id.ToString(),
                Date = DateTime.UtcNow
            };
            var events = new List<CloudEvent>()
            {
                new CloudEvent(
                    "/azuremessagingdemo/source", 
                    "AzureMessagingDemos.RecordUpdated", 
                    data)
            };

            await _sender.SendEventsAsync(events);

            Console.Write("+");
        }
    }
}
