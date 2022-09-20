using KafkaFunc;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;

[assembly: FunctionsStartup(typeof(Startup))]

namespace KafkaFunc
{
    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            //builder.Services.AddOptions<AzureADOptions>()
            //    .Configure<IConfiguration>((settings, configuration) =>
            //    {
            //        configuration.GetSection("AzureAD").Bind(settings);
            //    });

            //builder.Services.AddScoped<CachedSchemaRegistryClient>(s => new CachedSchemaRegistryClient(new SchemaRegistryConfig
            //{
            //    Url = "http://localhost:8081",
            //    BasicAuthUserInfo = "user:password"
            //}));
            //}
        }
    }
}
