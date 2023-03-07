using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace AzureDigitalTwinsUpdaterFunc;

public class ADTFunction
{
    private readonly ILogger _logger;

    public ADTFunction(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<ADTFunction>();
    }

    [Function("ADTFunc")]
    public void Run([EventHubTrigger("adt", Connection = "EventHubConnectionString")] string[] inputs)
    {
        var exceptions = new List<Exception>();
        foreach (var input in inputs)
        {
            try
            {
                _logger.LogTrace("Function processing message: {Input}", input);
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
