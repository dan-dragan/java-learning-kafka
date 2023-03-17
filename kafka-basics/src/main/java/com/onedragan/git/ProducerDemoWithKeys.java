package com.onedragan.git;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //System.out.println("hello world from kafka producer demo");
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        //create kafka producer properties
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers","192.168.1.150:9092");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.150:9092");
        //these help what kind of values we are sending to kafka and how to make them into bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create kafka producer itself
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i=0;i<10;i++) {
            //create a producer record
            String topic = "first_topic";
            String value = "hello world, this is message #"+Integer.toString(i)+" from java producer";
            String key = "id_"+Integer.toString(i);
            ProducerRecord<String, String> record   = new ProducerRecord<>(topic,key, value);
            logger.info("Key: " + key);
            //send data - !! this is asynchronous, but we made it sync for experiment purposes
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully send or an exception is thrown
                    if (e == null) {
                        //the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else {
                        //something failed
                        logger.error("Error while producing:", e.getMessage());
                    }//else
                }//onCompletion
            }/*anonymous callback */).get();//don't  do this in production
        }//for
        //flush and close
        producer.flush();
        producer.close();
    }
}
