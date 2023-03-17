package com.onedragan.git;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class ElasticSearchConsummer {

    public static RestHighLevelClient createClient(){
        //https://entui8cyr:zc3ersu3cs@rhs-com-search-2970815888.us-east-1.bonsaisearch.net:443
        String hostname = "**********.us-east-1.bonsaisearch.net";
        String username = "**********";
        String password = "**********";

        //do this against a remote ES, like Bonsai provides
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username,password));
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"));
        builder.setHttpClientConfigCallback( new RestClientBuilder.HttpClientConfigCallback(){
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder){
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        builder.build();
        RestHighLevelClient client;
        client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String,String> createConsummer(String topicName){

        //create kafka consumer properties
        Properties properties = new Properties();

        String bootstrapServers = "192.168.1.150:9092";
        String groupId = "kafka-demo-elasticsearch";
        String autoOffsetResetCfg = "earliest"; //latest/none
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        //these help what kind of values we are sending to kafka and how to make them into bytes
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetCfg);



        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton(topicName));
        return consumer;
    }
    private static JsonParser jsonParser= new JsonParser();
    private static String ExtractIDFromTweet(String tweetJson){
        if(tweetJson!=null) {
            try {
                //gson library
                JsonObject jobject = jsonParser.parse(tweetJson)
                        .getAsJsonObject();
                jobject = jobject.getAsJsonObject("data");
                String result = jobject.get("id").getAsString();
                return result;
            }catch ( IllegalStateException isex){
                isex.printStackTrace();
                return null;
            }
        }else {
            return null;
        }
    }
    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsummer.class);
        RestHighLevelClient client = createClient();

        String topicName= "twitter_tweets";
        KafkaConsumer<String,String> consumer = createConsummer(topicName);

        //poll for new data
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record : records){
                logger.info("Key: "+record.key()+", Value: "+record.value());
                logger.info("Partition: "+record.partition()+", Offset: "+record.offset());
                //we also insert data to ElasticSearch
                String jsonString = record.value();

                //if we were to run this code twice, we would get duplicated records in ES
                //so we need to make the consumer idempotent
                //that means passing an id to ES

                //2 strategies for generating it
                //1. kafka generic ID
                //String idElasticSearch = record.topic()+"_"+record.partition()+"_"+record.offset();

                //2. use a domain generated id, in twitter
                String idElasticSearch = ExtractIDFromTweet(record.value());
                if(idElasticSearch!=null) {
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", idElasticSearch);
                    indexRequest.source(jsonString, XContentType.JSON);

                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    String id = indexResponse.getId();
                    logger.info("indexResponse id is #" + id);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }//for
        }//while

       //client.close();
    }
}
