package com.bryancondor.kafka_demo;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {

  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

  public static void main(String[] args) {

    var consumer = new KafkaConsumer<String, Person>(getProperties());

    consumer.subscribe(List.of("kafka-demo.topic4"));

    while (true) {
      logger.info("Polling");

      ConsumerRecords<String, Person> records = consumer.poll(Duration.ofMillis(1000));

      for (ConsumerRecord<String, Person> record : records) {
        logger.info(record.toString());
      }
    }
  }

  private static Properties getProperties() {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

//    properties.setProperty("key.deserializer", StringSerializer.class.getName());
//    properties.setProperty("value.deserializer", StringSerializer.class.getName());

    properties.setProperty("key.deserializer", StringDeserializer.class.getName());
    properties.setProperty("value.deserializer", KafkaJsonSchemaDeserializer.class.getName());
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("group.id", "my-consumer-group-id");

    properties.setProperty("schema.registry.url", "http://localhost:8081");

    return properties;
  }
}
