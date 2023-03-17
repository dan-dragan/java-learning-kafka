package com.onedragan.git;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
public class ProducerDemo {
    public static void main(String[] args) {
        //System.out.println("hello world from kafka producer demo");
        //create kafka producer properties
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers","192.168.1.150:9092");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.150:9092");
        //these help what kind of values we are sending to kafka and how to make them into bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create kafka produce itself
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String, String> record   = new ProducerRecord<>("twitter_tweets", "hello world from java producer");
        //send data - !! this is asynchronous
        producer.send(record);

        //flush and close
        producer.flush();
        producer.close();
    }
}
