package com.bryancondor.kafka_demo;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

  private static final Logger logger = LoggerFactory.getLogger(Producer.class);

  public static void main(String[] args) {
    Properties properties = getProperties();

    var producer = new KafkaProducer<String, Person>(properties);
    var person = new Person();
    person.setId(1);
    person.setName("Alexander");
    person.setLastname("Condor");
    person.setNickname("nickname3");


    var producerRecord = new ProducerRecord<String, Person>("kafka-demo.topic4", person);

    producer.send(producerRecord, new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
          logger.info(metadata.toString());
        } else {
          exception.printStackTrace();
        }
      }
    });

    producer.flush();
    producer.close();
  }

  private static Properties getProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("acks", "1");
    properties.setProperty("auto.register.schemas", "false");

//    properties.setProperty("key.serializer", StringSerializer.class.getName());
//    properties.setProperty("value.serializer", StringSerializer.class.getName());

    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", KafkaJsonSchemaSerializer.class.getName());
    properties.setProperty("schema.registry.url", "http://localhost:8081");

    return properties;
  }
}
