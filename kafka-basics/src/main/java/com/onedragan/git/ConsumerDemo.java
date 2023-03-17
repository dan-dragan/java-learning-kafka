package com.onedragan.git;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        //create kafka consumer properties
        Properties properties = new Properties();

        String bootstrapServers = "192.168.1.150:9092";
        String groupId = "java-consumer-demo";
        String autoOffsetResetCfg = "earliest"; //latest/none
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        //these help what kind of values we are sending to kafka and how to make them into bytes
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetCfg);

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //subscribe consumer
        String topicName= "first_topic";
        consumer.subscribe(Collections.singleton(topicName));
        //consumer.subscribe(Array.asList(topicName, topicName2,...));

        //poll for new data
        //messages may have one or many records
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record : records){
                logger.info("Key: "+record.key()+", Value: "+record.value());
                logger.info("Partition: "+record.partition()+", Offset: "+record.offset());
            }//for
        }//while
    }//main
}//class
