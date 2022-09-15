using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using KafkaDemos;
using Microsoft.Azure.WebJobs.Extensions.Kafka;
using Newtonsoft.Json;

var localSchemaRegistry = new LocalSchemaRegistry(CarRecord.SchemaText);

// Create car avro file
var avroSerializer = new AvroSerializer<CarRecord>(localSchemaRegistry);
var avroBytesWrite = await avroSerializer.SerializeAsync(new CarRecord()
{
    CarID = "12345",
    Model = "Focus",
    Manufacturer = "Ford",
    Year = 2014
}, SerializationContext.Empty);

File.WriteAllBytes("car.avro", avroBytesWrite);

// Read car avro file
var avroBytesRead = await File.ReadAllBytesAsync("car.avro");

var avroDeserializer = new AvroDeserializer<GenericRecord>(localSchemaRegistry);
var genericRecord = await avroDeserializer.DeserializeAsync(avroBytesRead, false, SerializationContext.Empty);

var properties = new Dictionary<string, object>();
foreach (var field in genericRecord.Schema.Fields)
{
    if (genericRecord.TryGetValue(field.Name, out var value))
    {
        properties[field.Name] = value;
    }
}

var carJson = JsonConvert.SerializeObject(properties);
Console.WriteLine(carJson);

// Setup local Kafka using these instructions:
// https://docs.confluent.io/platform/current/platform-quickstart.html#cp-quick-start-docker
// 1) docker-compose up -d
// 2) http://localhost:9021/clusters
// 3) Create topic "cars" with schema from CarRecord.cs
var registry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
{
    Url = "http://localhost:8081",
    BasicAuthUserInfo = "user:password"
});

var avroDeserializerWithRegistry = new AvroDeserializer<GenericRecord>(registry);
var record = await avroDeserializerWithRegistry.DeserializeAsync(avroBytesRead, false, SerializationContext.Empty);

Console.WriteLine(record.GetValue(0));

// 4) Create topic "car-repairs" with schema CarRepairHistory.json
// "com.jannemattila.carrepairhistory"
var subjects = await registry.GetAllSubjectsAsync();
foreach (var subject in subjects)
{
    Console.WriteLine(subject);
}

var registeredSchema = (await registry.GetLatestSchemaAsync("car-repairs-value")) ?? throw new Exception("Schema not found");
var schema = Avro.Schema.Parse(registeredSchema.SchemaString);
var avroSerializerGeneric = new AvroSerializer<GenericRecord>(registry);
