package com.onedragan.git;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;

/*
 * Sample code to demonstrate the use of the Filtered Stream endpoint
 * */
public class TwitterProducer {
    class TwitterClient {
        String bearerToken = "";
        Map<String, String> rules = null;
        int tweetCount=10;
        String rulesURI= "https://api.twitter.com/2/tweets/search/stream/rules";
        Logger logger = LoggerFactory.getLogger(TwitterClient.class.getName());
        public TwitterClient(String bearerToken, Map<String, String> rules) {
            this.bearerToken = bearerToken;
            this.rules = rules;
            this.tweetCount = 10;

        }

        public TwitterClient(String bearerToken, Map<String, String> rules, int tweetCount) {
            this.bearerToken = bearerToken;
            this.rules = rules;
            this.tweetCount = tweetCount;
        }

        /*
         * Helper method to setup rules before streaming data
         * */
        private void setupRules() throws IOException, URISyntaxException {
            logger.info("Setting up rules...");
            List<String> existingRules = getRules();
            if (existingRules.size() > 0) {
                deleteRules(existingRules);
            }
            createRules();
            logger.info("...rules set up completed.");
        }

        /*
         * Helper method to create rules for filtering
         * */
        private void createRules() throws URISyntaxException, IOException {
            HttpClient httpClient = HttpClients.custom()
                    .setDefaultRequestConfig(RequestConfig.custom()
                            .setCookieSpec(CookieSpecs.STANDARD).build())
                    .build();

            URIBuilder uriBuilder = new URIBuilder(rulesURI);

            HttpPost httpPost = new HttpPost(uriBuilder.build());
            httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
            httpPost.setHeader("content-type", "application/json");
            StringEntity body = new StringEntity(getFormattedString("{\"add\": [%s]}", rules));
            httpPost.setEntity(body);
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            if (null != entity) {
                System.out.println(EntityUtils.toString(entity, "UTF-8"));
            }
        }

        /*
         * Helper method to get existing rules
         * */
        private List<String> getRules() throws URISyntaxException, IOException {
            List<String> rules = new ArrayList<>();
            HttpClient httpClient = HttpClients.custom()
                    .setDefaultRequestConfig(RequestConfig.custom()
                            .setCookieSpec(CookieSpecs.STANDARD).build())
                    .build();

            URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

            HttpGet httpGet = new HttpGet(uriBuilder.build());
            httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
            httpGet.setHeader("content-type", "application/json");
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (null != entity) {
                JSONObject json = new JSONObject(EntityUtils.toString(entity, "UTF-8"));
                if (json.length() > 1) {
                    JSONArray array = (JSONArray) json.get("data");
                    for (int i = 0; i < array.length(); i++) {
                        JSONObject jsonObject = (JSONObject) array.get(i);
                        rules.add(jsonObject.getString("id"));
                    }
                }
            }
            return rules;
        }

        /*
         * Helper method to delete rules
         * */
        private void deleteRules(List<String> existingRules) throws URISyntaxException, IOException {
            HttpClient httpClient = HttpClients.custom()
                    .setDefaultRequestConfig(RequestConfig.custom()
                            .setCookieSpec(CookieSpecs.STANDARD).build())
                    .build();

            URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

            HttpPost httpPost = new HttpPost(uriBuilder.build());
            httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
            httpPost.setHeader("content-type", "application/json");
            StringEntity body = new StringEntity(getFormattedString("{ \"delete\": { \"ids\": [%s]}}", existingRules));
            httpPost.setEntity(body);
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            if (null != entity) {
                System.out.println(EntityUtils.toString(entity, "UTF-8"));
            }
        }

        private String getFormattedString(String string, List<String> ids) {
            StringBuilder sb = new StringBuilder();
            if (ids.size() == 1) {
                return String.format(string, "\"" + ids.get(0) + "\"");
            } else {
                for (String id : ids) {
                    sb.append("\"" + id + "\"" + ",");
                }
                String result = sb.toString();
                return String.format(string, result.substring(0, result.length() - 1));
            }
        }

        private String getFormattedString(String string, Map<String, String> rules) {
            StringBuilder sb = new StringBuilder();
            if (rules.size() == 1) {
                String key = rules.keySet().iterator().next();
                return String.format(string, "{\"value\": \"" + key + "\", \"tag\": \"" + rules.get(key) + "\"}");
            } else {
                for (Map.Entry<String, String> entry : rules.entrySet()) {
                    String value = entry.getKey();
                    String tag = entry.getValue();
                    sb.append("{\"value\": \"" + value + "\", \"tag\": \"" + tag + "\"}" + ",");
                }
                String result = sb.toString();
                return String.format(string, result.substring(0, result.length() - 1));
            }
        }

        /*
         * This method calls the filtered stream endpoint and streams Tweets from it
         * */
        private void connectStream(Callback callback) throws IOException, URISyntaxException {

            logger.info("Requesting data from filtered stream....");
            HttpClient httpClient = HttpClients.custom()
                    .setDefaultRequestConfig(RequestConfig.custom()
                            .setCookieSpec(CookieSpecs.STANDARD).build())
                    .build();

            URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream");

            HttpGet httpGet = new HttpGet(uriBuilder.build());
            httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));

            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (null != entity) {
                logger.info("...successfully got response from stream...");
                BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
                int count=0;
                String line=null;
                do{
                    line = reader.readLine();
                    if(line != null) {
                        logger.info("message #"+Integer.toString(count)+" received.");
                        callback.onRecord(line,null);
                        count++;
                    }
                } while((line != null)&&(count<tweetCount));
                logger.info("...done consuming.");
            }else{
                logger.info("...got null entity answer from stream, done consuming.");
            }
        }//connectStream()
    }//class TwitterClient
    // To set your enviornment variables in your terminal run the following line:
    // export 'BEARER_TOKEN'='<your_bearer_token>'
    //we get this from our twitter developer account
    static String bearerToken = "**********";
    public interface Callback {
        void onRecord(String var1, Exception var2);
    }
    public void run(){
        Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
        //create twitter client
        logger.info("Setup...");
        Map<String, String> rules = new HashMap<>();
        rules.put("jammer", "jammer");
        TwitterClient client = new TwitterClient(bearerToken,rules);
        try {
            client.setupRules();
        }catch (IOException ioex){
            ioex.printStackTrace();
        }catch (URISyntaxException urisex){
            urisex.printStackTrace();
        }
        //create kafka producer
        KafkaProducer<String,String> producer = createKafkaProducer();
        logger.info("...setup completed, start consuming...");
        //read tweets
        try {
            client.connectStream(new Callback() {
                @Override
                public void onRecord(String msg, Exception ex) {
                    logger.info("Sending tweet["+msg+"] to topic [twitter_tweets]...");
                    producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new org.apache.kafka.clients.producer.Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e!=null){
                                logger.error("Exception caught while sending to kafka", e);
                            }
                        }
                    });
                    logger.info("...done!");
                }
            });
        }catch (IOException ioex){
            ioex.printStackTrace();
        }catch (URISyntaxException urisex){
            urisex.printStackTrace();
        }
        logger.info("...done consuming.");

        producer.flush();
        producer.close();

        logger.info("End of twitter producer.");
    }
    public KafkaProducer<String, String> createKafkaProducer(){
        //create kafka producer properties
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers","192.168.1.150:9092");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.150:9092");
        //these help what kind of values we are sending to kafka and how to make them into bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create kafka produce itself
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;
    }
    public static void main(String args[]) throws IOException, URISyntaxException {
        TwitterProducer producer = new TwitterProducer();
        producer.run();
    }

}//class TwitterProducer
