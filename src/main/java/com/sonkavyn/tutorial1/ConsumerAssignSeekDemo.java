package com.sonkavyn.tutorial1;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class ConsumerAssignSeekDemo {
  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    TopicPartition partitionToReadFrom = new TopicPartition("first_topic", 0);
    consumer.assign(List.of(partitionToReadFrom));
    long offsetToReadFrom = 15;
    consumer.seek(partitionToReadFrom, offsetToReadFrom);
    boolean keepOnReading = true;
    int messagesToRead = 6;
    int readMessages = 0;
    while (keepOnReading) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord record : records) {
        log.info(
            "Key: {}. Value: {}. Partition: {}. Offset: {}",
            record.key(),
            record.value(),
            record.partition(),
            record.offset());
        readMessages++;
        if(readMessages>=messagesToRead){
          keepOnReading = false;
          break;
        }
      }
    }
  }
}
