package com.personali.kafka;

import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by orsher on 5/7/18.
 *
 * Handles saving, retrieving and deleting records from an H2 locally persistent db.
 */
public class LocalRecordPersistentStore<K,V> {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LocalRecordPersistentStore.class);
    private Connection conn;

    /**
     * Initialize the local persistent store
     *
     * @param recreateDB Should the local db be truncated on instantiation
     * @param name Name of the local file
     * @throws SQLException
     */
    protected LocalRecordPersistentStore(boolean recreateDB, String name) throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:~/"+name+";" +
                                            "INIT=CREATE SCHEMA IF NOT EXISTS recordPersistence\\; " +
                                            "SET SCHEMA recordPersistence\\; " +
                                            (recreateDB ? "DROP TABLE IF EXISTS records\\; " : "") +
                                            "CREATE TABLE IF NOT EXISTS records(id bigint auto_increment, topic varchar(250), key BINARY, value BINARY)");
    }

    /**
     * Writes a new record to the local db
     *
     * @param topic Topic name
     * @param key Record serialized key
     * @param value Record serialized value
     * @throws SQLException
     */
    protected void writeRecord(String topic, byte[] key, byte[] value) throws SQLException{
        logger.debug("Writing a record to local record persistent store");
        PreparedStatement prep = conn.prepareStatement(
                "insert into records (topic,key,value) values (?,?,?)");
        prep.setString(1, topic);
        prep.setBytes(2, key);
        prep.setBytes(3, value);
        prep.execute();
    }

    /**
     * Read the first limit records in a FIFO order
     *
     * @param limit Number of max records to fetch from store
     * @return List of serialized wrapped records
     * @throws SQLException
     */
    protected List<TopicKeyValueBytes> readRecords(int limit) throws SQLException {
        logger.debug("Reading records from local record persistent store");
        PreparedStatement prep = conn.prepareStatement("select id, topic, key, value from records order by 1 limit ?");
        prep.setInt(1, limit);
        ResultSet results = prep.executeQuery();
        List<TopicKeyValueBytes> records = new ArrayList<>();
        while (results.next()){
            records.add(new TopicKeyValueBytes(results.getLong(1),results.getString(2),results.getBytes(3),results.getBytes(4)));
        }
        return records;
    }

    /**
     * Delete a recorde from the local persistent store
     *
     * @param id Id of the record row in the local store
     * @throws SQLException
     */
    protected void deleteRecord(long id) throws SQLException {
        logger.debug("Deleting record from local record persistent store");
        PreparedStatement prep = conn.prepareStatement("delete from records where id = ?");
        prep.setLong(1, id);
        prep.execute();

    }

}
