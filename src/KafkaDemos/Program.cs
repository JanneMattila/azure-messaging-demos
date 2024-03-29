﻿using Avro;
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

var carRepairHistoryRegisteredSchema = (await registry.GetLatestSchemaAsync("car-repairs-value")) ?? throw new Exception("Schema not found");
var carHistorySchema = (RecordSchema)Avro.Schema.Parse(carRepairHistoryRegisteredSchema.SchemaString);
var carRepairHistorySchema = (RecordSchema)carHistorySchema["history"].Schema;

var carHistoryItem = new GenericRecord(carRepairHistorySchema);
carHistoryItem.Add("date", "2022-09-15");
carHistoryItem.Add("cost", 15000.67);

var carHistoryRecord = new GenericRecord(carHistorySchema);
carHistoryRecord.Add("carid", "12345");
carHistoryRecord.Add("history", carHistoryItem);

var avroSerializerCarHistory = new AvroSerializer<GenericRecord>(registry);

var carHistoryData = await avroSerializerCarHistory.SerializeAsync(carHistoryRecord, SerializationContext.Empty);
Console.WriteLine($"Data size: {carHistoryData.Length}");
File.WriteAllBytes("carhistory.avro", carHistoryData);

// 5) Submit data to "cars" topic in Avro format
var carsRegisteredSchema = (await registry.GetLatestSchemaAsync("cars-value")) ?? throw new Exception("Schema not found");
var carsSchema = (RecordSchema)Avro.Schema.Parse(carsRegisteredSchema.SchemaString);

var id = DateTime.Now.ToString("yyyyMMddmmhhss");
var carItem = new GenericRecord(carsSchema);
carItem.Add("carid", id);
carItem.Add("manufacturer", "Daewoo");
carItem.Add("model", "Matiz");
carItem.Add("year", 2004L);

var config = new Dictionary<string, string>
{
    { "bootstrap.servers", "localhost:9092" },
    { "security.protocol", "PLAINTEXT" },
    { "sasl.mechanism", "PLAIN" },
    { "sasl.username", "$ConnectionString" },
    { "sasl.password", "password" }
};

var localSchemaRegistryCar = new LocalSchemaRegistry(carsRegisteredSchema.SchemaString);
var avroSerializerCar = new AvroSerializer<GenericRecord>(localSchemaRegistryCar);

var carData = await avroSerializerCar.SerializeAsync(carItem, SerializationContext.Empty);

var sender = new ProducerBuilder<string, byte[]>(config).Build();

var deliveryResultCar = await sender.ProduceAsync("cars", new Message<string, byte[]> { Key = id.ToString(), Value = carData });
Console.WriteLine(deliveryResultCar.Key);

var deliveryResultCarHistory = await sender.ProduceAsync("car-repairs", new Message<string, byte[]> { Key = id.ToString(), Value = carHistoryData });
Console.WriteLine(deliveryResultCarHistory.Key);

// 6) Clean up
// docker-compose down
