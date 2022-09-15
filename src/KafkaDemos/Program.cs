using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using KafkaDemos;
using Microsoft.Azure.WebJobs.Extensions.Kafka;
using Newtonsoft.Json;

var localSchemaRegistry = new LocalSchemaRegistry(CarRecord.SchemaText);

// Create avro file
var avroSerializer = new AvroSerializer<CarRecord>(localSchemaRegistry);
var avroBytesWrite = await avroSerializer.SerializeAsync(new CarRecord()
{
    CarID = "12345",
    Model = "Focus",
    Manufacturer = "Ford",
    Year = 2014
}, SerializationContext.Empty);

File.WriteAllBytes("car.avro", avroBytesWrite);

// Read avro file
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
