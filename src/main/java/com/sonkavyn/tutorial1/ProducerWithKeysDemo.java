package com.sonkavyn.tutorial1;

import java.util.Properties;
import java.util.stream.IntStream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class ProducerWithKeysDemo {
  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    IntStream.iterate(1, i -> i < 10, i -> i + 1)
        .mapToObj(i -> new ProducerRecord<>("first_topic", "id_" + i, i + "A"))
        .forEach(
            record ->
                producer.send(
                    record,
                    (metadata, exception) -> {
                      if (exception == null) {
                        log.info(
                            "Received new metadata. \n Topic: {}. \n Partition: {}. \n Offset: {}",
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset());

                      } else {
                        log.error(exception.getMessage(), exception);
                      }
                    })
        );
    producer.flush();
    producer.close();
  }
}
