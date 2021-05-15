package com.jannemattila;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.Properties;

public class App 
{
    public static void main(String[] args) throws Exception
    {
        var topic = "topic1";
        var properties = "Local.properties";
        if (args.length == 2) {
            topic = args[0];
            properties = args[1];
        }   

        System.out.println("Kafka messaging demo");
        var configurationFile = Path.of(properties).toRealPath().toString();
        System.out.println("Using configuration: " + configurationFile);
        var props = new Properties();
        var inputStream = new FileInputStream(configurationFile);
        props.load(inputStream);
        inputStream.close();

        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        var producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++)
        {
            producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i)));
        }

        producer.close();
    }
}
