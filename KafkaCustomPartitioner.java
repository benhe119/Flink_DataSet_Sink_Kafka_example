package com.yngwiewang;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class KafkaCustomPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPatitions = partitions.size();
        String strKey = (String) key;
        int intKey = Integer.parseInt(strKey.substring(9, 15));
        return intKey % numPatitions;
    }

    @Override
    public void close() {
    }

}
