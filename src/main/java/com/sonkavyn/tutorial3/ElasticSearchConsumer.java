package com.sonkavyn.tutorial3;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

@Slf4j
public class ElasticSearchConsumer {
  public static void main(String[] args) throws IOException {

    RestHighLevelClient client = createClient();
    //    CreateIndexRequest create = new CreateIndexRequest("twitter");
    //    client.indices().create(create, RequestOptions.DEFAULT);

    KafkaConsumer<String, String> consumer = createConsumer();

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      log.info("read {} ", records.count() );
      if (!records.isEmpty()) {
        BulkRequest request = new BulkRequest();
        for (ConsumerRecord<String, String> r : records) {
          request.add(
              new IndexRequest("twitter").opType(OpType.INDEX).source(r, XContentType.JSON).id(r.topic() + "_" +  r.partition() + "_" + r.key() ));
        }
        client.bulk(request, RequestOptions.DEFAULT);

        consumer.commitSync();

        log.info("committed");
      }
    }
  }

  private static RestHighLevelClient createClient() {
    BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
    basicCredentialsProvider.setCredentials(
        AuthScope.ANY, new UsernamePasswordCredentials("uhhlf7zs1z", "zp3bttc8uz"));
    RestClientBuilder builder =
        RestClient.builder(
                new HttpHost("kafka-course-8244065962.eu-central-1.bonsaisearch.net", 443, "https"))
            .setHttpClientConfigCallback(
                httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider));
    return new RestHighLevelClient(builder);
  }

  private static KafkaConsumer<String, String> createConsumer() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "elastic-push");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(List.of("twitter_tweets_2"));
    return consumer;
  }
}
