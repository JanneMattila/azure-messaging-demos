using Azure;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using AzureDigitalTwinsUpdaterFunc.Interfaces;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace AzureDigitalTwinsUpdaterFunc;

public class ADTFunction
{
    private readonly ILogger _logger;
    private readonly DigitalTwinsClient _client;

    public ADTFunction(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<ADTFunction>();

        var instanceUrl = Environment.GetEnvironmentVariable("ADT_INSTANCE_URL");

        if (string.IsNullOrEmpty(instanceUrl))
        {
            _logger.LogError("ADT_INSTANCE_URL is not set");

            throw new ArgumentNullException("ADT_INSTANCE_URL is not set");
        }

        _client = new DigitalTwinsClient(new Uri(instanceUrl), new DefaultAzureCredential());
    }

    [Function("ADTFunc")]
    public async Task Run([EventHubTrigger("adt", Connection = "EventHubConnectionString")] string[] inputs)
    {
        var exceptions = new List<Exception>();
        foreach (var input in inputs)
        {
            try
            {
                _logger.LogTrace("Function processing message: {Input}", input);
                var digitalTwinUpdateRequest = JsonSerializer.Deserialize<DigitalTwinUpdateRequest>(input);

                _logger.LogTrace("Fetching digital twin with ID: {ID}", digitalTwinUpdateRequest.ID);

                var digitalTwin = await _client.GetDigitalTwinAsync<BasicDigitalTwin>(digitalTwinUpdateRequest.ID).ConfigureAwait(false);

                var digitalTwinUpdate = new JsonPatchDocument();

                digitalTwinUpdate.AppendReplace("/OPCUANodeValue", digitalTwinUpdateRequest.Value);

                _logger.LogTrace("Updating digital twin with ID: {ID} with value: {Value}", digitalTwinUpdateRequest.ID, digitalTwinUpdateRequest.Value);
                await _client.UpdateDigitalTwinAsync(digitalTwinUpdateRequest.ID, digitalTwinUpdate, ETag.All).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Exception while processing message");
                exceptions.Add(ex);
            }
        }

        if (exceptions.Count > 1)
        {
            throw new AggregateException(exceptions);
        }

        if (exceptions.Count == 1)
        {
            throw exceptions.Single();
        }
    }
}
