package com.sonkavyn.tutorial1;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class ConsumerWithThreadsDemo {
  @SneakyThrows
  public static void main(String[] args) {
    CountDownLatch latch = new CountDownLatch(1);
    ConsumerRunnable consumerRunnable =
        new ConsumerRunnable("localhost:9092", "my-sixth-app", "first_topic", latch);
    new Thread(consumerRunnable).start();
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  consumerRunnable.shutDown();
                  try {
                    latch.await();
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                }));
    latch.await();
  }
}

@Slf4j
class ConsumerRunnable implements Runnable {
  private final CountDownLatch latch;
  private final KafkaConsumer<String, String> consumer;

  public ConsumerRunnable(
      String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
    this.latch = latch;
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    this.consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(List.of(topic));
  }

  @Override
  public void run() {
    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        records.forEach(
            r ->
                log.info(
                    "Key: {}. Value: {}. Partition: {}. Offset: {}",
                    r.key(),
                    r.value(),
                    r.partition(),
                    r.offset()));
      }
    } catch (WakeupException e) {
      log.error("Received shutdown signal!");
    } finally {
      consumer.close();
      latch.countDown();
    }
  }

  public void shutDown() {
    consumer.wakeup();
  }
}
