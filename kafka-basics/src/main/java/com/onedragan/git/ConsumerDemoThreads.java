package com.onedragan.git;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThreads {
    public class RunnableConsumer implements Runnable{
        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer ;

        private final Logger logger = LoggerFactory.getLogger(RunnableConsumer.class);
        public RunnableConsumer(CountDownLatch latch,
                                String bootstrapServers,
                                String groupId,
                                String autoOffsetResetCfg,
                                String topic){
            this.latch=latch;
            //create kafka consumer properties
            Properties properties = new Properties();


            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            //these help what kind of values we are sending to kafka and how to make them into bytes
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetCfg);

            this.consumer = new KafkaConsumer<String, String>(properties);

            String topicName= topic;
            consumer.subscribe(Collections.singleton(topicName));
        }
        @Override
        public void run() {
            try {
                //poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }//for
                }//while
            } catch (WakeupException  wuex){
                logger.info("Received shutdown signal.");
            }finally {
                consumer.close();
                //tell main caller we're done with the thread
                latch.countDown();
            }
        }
        public void shutdown() {
            //the wakeup() method interrupts consumer.poll(). It will throw a wakeup exception
            consumer.wakeup();
        }
    }
    private ConsumerDemoThreads(){

    }
    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoThreads.class);
        String bootstrapServers = "192.168.1.150:9092";
        String groupId = "java-consumer-demo-threads";
        String autoOffsetResetCfg = "earliest"; //latest/none
        String topic = "first_topic"; //latest/none

        CountDownLatch latch= new CountDownLatch(1);
        logger.info("Creating the RunnableConsumer...");
        RunnableConsumer myRunnableConsumer = new RunnableConsumer(latch,
                bootstrapServers,
                groupId,
                autoOffsetResetCfg,
                topic);
        logger.info("...done creating the RunnableConsumer");
        logger.info("Running the RunnableConsumer...");
        Thread myThread = new Thread(myRunnableConsumer);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught shutdown hook, begin shutdown...");
            myRunnableConsumer.shutdown();
            try {
                latch.await();
            }catch (InterruptedException iex) {
                iex.printStackTrace();
                logger.error("...The application is shutting down with exceptions!", iex);
            } finally{
                logger.info("...The application has shut down cleanly.");
            }
        }));

        System.out.println("Press <return> to exit");
        try {
            System.in.read();
            myRunnableConsumer.shutdown();
        } catch (IOException e) {
            logger.error("...The application is shutting down with exceptions!", e);
            throw new RuntimeException(e);
        }
        try {
            latch.await();
        }catch (InterruptedException iex) {
            iex.printStackTrace();
            logger.error("The application got interrupted!", iex);
        } finally{
            logger.info("...done running the RunnableConsumer");
        }

    }
    public static void main(String[] args) {

        ConsumerDemoThreads consumerDemoThreads = new ConsumerDemoThreads();
        consumerDemoThreads.run();

    }//main

}//class
