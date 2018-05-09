package com.personali.kafka;

/**
 * Created by orsher on 5/7/18.
 *
 * A model class for serialized records to be stored locally
 */
public class TopicKeyValueBytes {
    private long id;
    private String topic;
    private byte[] key;
    private byte[] value;

    public TopicKeyValueBytes(long id, String topic, byte[] key, byte[] value){
        this.id = id;
        this.key = key;
        this.value = value;
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getId() {
        return id;
    }
}
