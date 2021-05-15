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
        var properties = "Local.properties";
        if (args.length == 2) {
            properties = args[1];
        }

        System.out.println("Kafka messaging demo");
        var configurationFile = Path.of(properties).toRealPath().toString();
        System.out.println("Using configuration: " + configurationFile);
        var props = new Properties();
        var inputStream = new FileInputStream(configurationFile);
        props.load(inputStream);
        inputStream.close();
        
        var producer = new KafkaProducer<String, String>(props);
        producer.close();
    }
}
