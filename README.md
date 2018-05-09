# Kafka Clients Enhanced

This project provides wrappers around Apache Kafka official java client.
The wrappers bring best practice initial config for exactly once delivery
and extend the basic functionality for a more easy and resilient use.

Currently only KafkaProducerWrapper is supported.

## KafkaProducerWrapper

This wrapper provides out of the box config to support exactly once delivery.
When all of the native client retries failed, the wrapper stores the message in a local persistent store.
Periodically it retries to re-send them while preserving original messages order (only between the failed ones).

### Prerequisites
Java >= 8
Maven
Git

## Installing

git clone https://github.com/personali/kafka-clients-enhanced
cd kafka-clients-enhanced
mvn clean install

## Adding to project

Add the following dependency to your pom.xml:

    <dependency>
      <groupId>com.personali</groupId>
      <artifactId>kafka-clients-enhanced</artifactId>
      <version>1.0-SNAPSHOT</version>
    </dependency>

## Usage example

KafkaProducerWrapper producer = new KafkaProducerWrapper<String,Event>(
                "localhost:9092",
                "http://localhost:8081",
                "avro-topic",
                new org.apache.kafka.common.serialization.StringSerializer(),
                new io.confluent.kafka.serializers.KafkaAvroSerializer(),
                new org.apache.kafka.common.serialization.StringDeserializer(),
                new io.confluent.kafka.serializers.KafkaAvroDeserializer(),
                false
                );

producer.send("key1", avroObjectInstance);

## Authors

* **Or Sher**


