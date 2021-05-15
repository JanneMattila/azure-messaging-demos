package com.jannemattila;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
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

        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        var producer = new KafkaProducer<String, String>(props);

        for (var i = 0; i < 100; i++)
        {
            System.out.println("Send: " + i);
            producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i)));
        }

        producer.close();

        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var consumer = new KafkaConsumer<String, String>(props);

        var topics = new ArrayList<String>();
        topics.add(topic);

        consumer.subscribe(topics);
        for (var i = 0; i < 10; i++)
        {
            var messages = consumer.poll(Duration.ofSeconds(15));
            for (var message : messages) 
            {
                System.out.println("Receive: " + message.key());
            } 
        }

        consumer.close();
    }
}
