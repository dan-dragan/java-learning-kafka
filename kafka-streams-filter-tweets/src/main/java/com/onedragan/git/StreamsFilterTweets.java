package com.onedragan.git;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

    private static JsonParser jsonParser= new JsonParser();
    private static Integer extractFollowerCountFromTweet(String tweetJson){
        if(tweetJson!=null) {
            try {
                //gson library
                JsonObject jobject = jsonParser.parse(tweetJson)
                        .getAsJsonObject();
                jobject = jobject.getAsJsonObject("user");
                Integer count =jobject.get("followers_count").getAsInt();
                return count;
            }catch ( IllegalStateException isex){
                isex.printStackTrace();
                return -1;
            }catch ( NullPointerException npex) {
                npex.printStackTrace();
                return -1;
            }catch ( StreamsException sex) {
                sex.printStackTrace();
                return -1;
            }
        }else {
            return -1;
        }
    }
    public static void main(String[] args) {
        String bootstrapServers = "192.168.1.150:9092";
        String applicationIdConfig = "demo-kafka-streams";
        String topic = "twitter_tweets";
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationIdConfig);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String,String> inputTopic = streamsBuilder.stream(topic);
        KStream<String,String> filteredStream = inputTopic.filter(
                //filter for tweets from users which has a follower count of 10000 or greater  these are valuable users
                (k,jsonTweet) -> extractFollowerCountFromTweet(jsonTweet)>1000

       );
        filteredStream.to("important_tweets");

        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);

        //start our streams application
        kafkaStreams.start();
    }
}
