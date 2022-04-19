package com.personali.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by orsher on 5/7/18.
 *
 * An Apache KafkaProducer client wrapper which handles backing up failed messages
 * to a local store and retrying them periodically while keeping failed messages original order.
 */
public class KafkaProducerWrapper<K,V> {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaProducerWrapper.class);
    private KafkaProducer<K,V> producer;
    private String topic;
    private Serializer keySerializer;
    private Deserializer keyDeserializer;
    private Serializer valueSerializer;
    private Deserializer valueDeserializer;
    private String SCHEMA = "{\"name\": \"HealthCheck\",\"type\": \"record\",\"namespace\": \"com.personali.healt\",\"fields\": [{\"name\": \"type\",\"type\": \"string\"},{\"name\": \"status\",\"type\": \"string\"}]}";
    private Schema schema = Schema.parse(SCHEMA);
    LocalRecordPersistentStore<K,V> localRecordPersistentStore;
    private SchemaRegistryClient registryClient = null;

    /**
     * @param bootstrapServers kafka list of bootstrap servers
     * @param schemaRegistryUrl schema registry url for schema based serdes
     * @param topic kafka topic name to send messages to
     * @param keySerializer serialize object to serialize keys
     * @param valueSerializer serialize object to serialize values
     * @param keyDeserializer deserialize object to deserialize keys
     * @param valueDeserializer deserialize object to deserialize values
     * @param recreateLocalStore should the local store be truncated on initialization
     * @throws KafkaProducerWrapperException
     * @throws KafkaUnavailableException
     */
    public KafkaProducerWrapper(String bootstrapServers, String schemaRegistryUrl,
                                String topic, Serializer keySerializer, Serializer valueSerializer,
                                Deserializer keyDeserializer, Deserializer valueDeserializer,
                                Boolean recreateLocalStore, Boolean checkKafkaAvailability) throws KafkaProducerWrapperException, KafkaUnavailableException {
        this.topic = topic;

        //Creating a default best practice, exactly once producer configuration
        ////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////
        ////////////////// ----- hello Roy was here ----- //////////////////////
        ////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 10);
        props.put("retry.backoff.ms", 5000);
        props.put("key.serializer",keySerializer.getClass());
        props.put("value.serializer", valueSerializer.getClass());
        props.put("enable.idempotence", "true");
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("max.block.ms", 30000);

        //Configure wrapper serdes for local store usage
        this.keyDeserializer = keyDeserializer;
        this.keySerializer = keySerializer;
        this.valueDeserializer = valueDeserializer;
        this.valueSerializer = valueSerializer;
        this.keySerializer.configure(props,true);
        this.keyDeserializer.configure(props,true);
        this.valueSerializer.configure(props,false);
        this.valueDeserializer.configure(props,false);

        //Initialize Apache Kafka Producer Client
        producer = new KafkaProducer<K, V>(props);
        try {
            if (checkKafkaAvailability){
                if (valueSerializer.getClass().getName().contains("StringSerializer")) {
                    Future<RecordMetadata> metadata = producer.send((ProducerRecord<K, V>) new ProducerRecord<String, String>("KafkaHealthCheck", null, "healthCheck Event"), null);
                }
                else if (valueSerializer.getClass().getName().contains("KafkaAvroSerializer")){
                    registryClient = new CachedSchemaRegistryClient(schemaRegistryUrl,1000);
                    registryClient.register("HealthCheck", schema);
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("type", "KAFKA");
                    record.put("status", "OK");
                    Future<RecordMetadata> metadata = producer.send((ProducerRecord<K, V>) new ProducerRecord<String, GenericRecord>("AvroKafkaHealthCheck", null, record), null);
                }
            }

        }
        catch (Exception e){
            logger.error("Failed to Send HealthCheck Event", e);
            throw new KafkaUnavailableException("Failed to Send HealthCheck Event during KafkaProducer init!", e);
        }

        //Create and initialize local store
        try {
            localRecordPersistentStore = new LocalRecordPersistentStore<K, V>(recreateLocalStore,topic);
        }
        catch (SQLException e){
            logger.error("Could not create local record persistent store", e);
            throw new KafkaProducerWrapperException("Could not create local record persistent store",e);
        }

        //Start a scheduled worker to retry messages from local persistent store
        startScheduledLocalRecordPersistentStoreCheck(Optional.empty());
    }

    /**
     * A wrapper for the send method.
     * If an error occur, handleFirstException is delegated to handle the failure
     *
     * @param key Message key
     * @param value Massage value
     * @return
     */
    public Future<RecordMetadata> send(K key, V value) {
        return producer.send(
                new ProducerRecord<K, V>(topic, key, value),
                (record,e) -> {
                    if (e != null){
                        handleFirstExcpetion(record,e,topic,key,value);
                    }
                }
        );
    }

    /**
     * Internal send wrapper.
     * Should be used when the wrapper retries sending messages using it's own callback.
     *
     * @param key
     * @param value
     * @param callback
     * @return
     */
    private Future<RecordMetadata> send(K key, V value, Callback callback){
        return producer.send(new ProducerRecord<K, V>(topic, key, value),callback);
    }

    /**
     * Handles the first time a record submission failed by the original client.
     * Serializes record to bytes and save to local store.
     * If fails for some reason, it will log an error and the message would be lost.
     *
     * @param record Record metadata for the failed submission
     * @param e Exception that was thrown
     * @param topic Target topic name for the record
     * @param key Record key
     * @param value Record value
     */
    private void handleFirstExcpetion(RecordMetadata record, Exception e,String topic, K key, V value) {
        logger.error("Failed to produce message. Writing to local persistence store.", e);
        try {
            localRecordPersistentStore.writeRecord(
                    topic,
                    keySerializer.serialize(topic,key),
                    valueSerializer.serialize(topic,value)
            );
        } catch (SQLException sqle) {
            logger.error("Could not insert new record to local record persistent store", sqle);
        }
    }

    /**
     * Start a scheduled executor to poll local store for failed messages and retry
     *
     * @param interval Optional polling interval
     */
    private void startScheduledLocalRecordPersistentStoreCheck(Optional<Integer> interval){
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        Runnable task = () -> {
            try {
                checkLocalStore();
            } catch (Exception e) {
                logger.error("Scheduled local record persistent store check failed");
            }
        };
        executor.scheduleWithFixedDelay(task, interval.orElse(10), interval.orElse(10), TimeUnit.SECONDS);
    }

    /**
     * Checks local persistent store for failed records.
     * Send records again to Kafka in bulks and remove successful records from local persistent store.
     *
     * @throws SQLException
     */
    private void checkLocalStore() throws SQLException {
        logger.debug("Checking local record persistent store");
        List<TopicKeyValueBytes> records = localRecordPersistentStore.readRecords(100);

        //While there are still failed record in local persistent store
        //Resend them
        while (records.size() > 0){
            logger.info("Resending {} records from local persistent store.",records.size());
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            records.stream().forEach(record ->
                futures.add(
                    send((K)keyDeserializer.deserialize(record.getTopic(),record.getKey()),
                            (V)valueDeserializer.deserialize(record.getTopic(), record.getValue()),
                            //Resend callback
                            (r,e) -> {
                               if (e != null){
                                   logger.error("Failed resending message from local persistent store", e);
                               }
                               //If resend attempt was successful, remove record from local store
                               else{
                                   logger.debug("Successfully resent message from local persistent store. Removing from local store");
                                   try {
                                       localRecordPersistentStore.deleteRecord(record.getId());
                                   } catch (SQLException esql) {
                                       logger.error("Could not delete record from local persistent store. This may result with it being sent again.",esql);
                                   }
                               }
                            }
                        )
                )
            );

            //Wait for all records to be acknowledged/failed
            for (int i = 0; i < futures.size(); i++) {
                try {
                    futures.get(i).get();
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("Kafka producer returned a failed future", e);
                }
            }

            //TODO: If some records failed, wait until next schedule

            //Get next bulk of records
            records = localRecordPersistentStore.readRecords(100);
        }
    }

}
