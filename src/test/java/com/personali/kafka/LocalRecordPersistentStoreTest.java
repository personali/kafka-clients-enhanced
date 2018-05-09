package com.personali.kafka;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by orsher on 5/7/18.
 */
public class LocalRecordPersistentStoreTest {
    @Test
    public void writeAndReadRecords() throws Exception {
        LocalRecordPersistentStore persistent = new LocalRecordPersistentStore<String,String>(true,"topic1");

        persistent.writeRecord("topic1","key1".getBytes(),"value1".getBytes());
        persistent.writeRecord("topic2","key2".getBytes(),"value2".getBytes());
        List<TopicKeyValueBytes> records = persistent.readRecords(100);
        records.forEach((record) ->
                System.out.println(
                        String.format("%d %s %s %s", record.getId(),
                                record.getTopic(),new String(record.getKey()),new String (record.getValue()))));
        assertEquals(2,records.size());
    }

    @Test
    public void writeAndDeleteRecords() throws Exception {
        LocalRecordPersistentStore persistent = new LocalRecordPersistentStore<String,String>(true,"topic1");

        persistent.writeRecord("topic1","key1".getBytes(),"value1".getBytes());
        persistent.writeRecord("topic2","key2".getBytes(),"value2".getBytes());
        persistent.deleteRecord(1);
        List<TopicKeyValueBytes> records = persistent.readRecords(100);
        assertEquals(1,records.size());
    }
}