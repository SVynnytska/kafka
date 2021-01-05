package com.sonkavyn.tutorial2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class TwitterProducer {
  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run() {
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
    Client client = createTwitterClient(msgQueue);
    client.connect();
    KafkaProducer<String, String> producer = createKafkaProducer();
    while (!client.isDone()) {
      try {
        String message = msgQueue.poll(5, TimeUnit.SECONDS);
        log.info(message);
        if (message != null) {
          producer.send(
              new ProducerRecord<>("twitter_tweets_2", message),
              (metadata, exception) -> {
                if (exception != null) {
                  log.error(exception.getMessage(), exception);
                }
              });
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }
    }
    producer.close();
    log.info("End of Application");
  }

  private KafkaProducer<String, String> createKafkaProducer() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    properties.put(
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // lets assume order is important

    //    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    //    properties.put(ProducerConfig.LINGER_MS_CONFIG, 20);
    //    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024); // 32KB max batch size

    return new KafkaProducer<>(properties);
  }

  private BasicClient createTwitterClient(BlockingQueue<String> msgQueue) {
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    List<String> terms = List.of("trump");
    hosebirdEndpoint.trackTerms(terms);
    Authentication hosebirdAuth =
        new OAuth1(
            "rxHYkiuLn4e4kPzb0ztzHzENA",
            "HmNcysp28bywE7qhbRNYqVDxTbK5yPk3mziTY8phbrWNA7Gday",
            "2904362194-6rWO3sTg6snnJoKcoe4NoX4iEhwCI9VHsmkh9EZ",
            "PIRPTGa9kqOL6v2BUuSqtsegr5YSOJrj6zW9gkTK8pcG2");

    ClientBuilder builder =
        new ClientBuilder()
            .name("Hosebird-Client-01")
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

    return builder.build();
  }
}
