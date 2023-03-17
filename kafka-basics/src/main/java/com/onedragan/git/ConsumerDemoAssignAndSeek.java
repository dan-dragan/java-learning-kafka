package com.onedragan.git;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);

        //create kafka consumer properties
        Properties properties = new Properties();

        String bootstrapServers = "192.168.1.150:9092";
        String autoOffsetResetCfg = "earliest"; //latest/none
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        //these help what kind of values we are sending to kafka and how to make them into bytes
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetCfg);

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //we are not going to subscribe consumer, instead we will use assign and seek.
        //this is mostly used to replay data or fetch a specific message

        String topicName= "first_topic";
        int topicPartitionId =0;
        //assign
        TopicPartition partition = new TopicPartition(topicName,topicPartitionId);
        consumer.assign(Arrays.asList(partition));

        //seek
        long partitionOffsetToReadFrom=15L;
        consumer.seek(partition,partitionOffsetToReadFrom);

        int nRecordsToRead= 5;
        boolean keepOnReading=true;
        int nRecordsReadSoFar= 0;
        //poll for new data
        //messages may have one or many records
        while(keepOnReading){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records){
                nRecordsReadSoFar+=1;
                logger.info("Key: "+record.key()+", Value: "+record.value());
                logger.info("Partition: "+record.partition()+", Offset: "+record.offset());
                if(nRecordsReadSoFar==nRecordsToRead) {
                    keepOnReading = false;
                    break;
                }
            }//for
        }//while
    }//main
}//class
