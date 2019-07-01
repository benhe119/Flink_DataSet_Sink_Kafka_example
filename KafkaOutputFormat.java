package com.yngwiewang;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class KafkaOutputFormat implements OutputFormat<Tuple7<String, String, String, String, String, String, String>> {

    private String servers;
    private String topic;
    private String acks;
    private String retries;
    private String batchSize;
    private String bufferMemory;
    private String lingerMS;

    private Producer<String, String> producer;


    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.servers);
        props.put("topic", this.topic);
        props.put("acks", this.acks);
        props.put("retires", this.retries);
        props.put("batch.size", this.batchSize);
        props.put("linger.ms", this.lingerMS);
        props.put("buffer.memory", this.bufferMemory);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "com.yngwiewang.KafkaCustomPartitioner");

        producer = new KafkaProducer<>(props);
    }

    @Override
    public void writeRecord(Tuple7<String, String, String, String, String, String, Integer> record) throws IOException {
        producer.send(new ProducerRecord<>(this.topic, record.f0,
                String.join(",", record.f0, record.f1, record.f2, record.f3, record.f4, record.f5)));
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }


    static KafkaOutputFormatBuilder buildKafkaOutputFormat() {
        return new KafkaOutputFormatBuilder();
    }


    public static class KafkaOutputFormatBuilder {
        private final KafkaOutputFormat format;

        KafkaOutputFormatBuilder() {
            this.format = new KafkaOutputFormat();
        }

        KafkaOutputFormatBuilder setBootstrapServers(String val) {
            format.servers = val;
            return this;
        }

        KafkaOutputFormatBuilder setTopic(String val) {
            format.topic = val;
            return this;
        }


        KafkaOutputFormatBuilder setAcks(String val) {
            format.acks = val != null ? val : "all";
            return this;
        }

        KafkaOutputFormatBuilder setRetries(String val) {
            format.retries = val != null ? val : "3";
            return this;
        }

        KafkaOutputFormatBuilder setBatchSize(String val) {
            format.batchSize = val != null ? val : "16384";
            return this;
        }

        KafkaOutputFormatBuilder setLingerMs(String val) {
            format.lingerMS = val != null ? val : "1000";
            return this;
        }

        KafkaOutputFormatBuilder setBufferMemory(String val) {
            format.bufferMemory = val != null ? val : "33554432";
            return this;
        }


        KafkaOutputFormat finish() {
            if (format.servers == null) {
                throw new IllegalArgumentException("required parameter not found: KafkaBrokerList");
            }
            if (format.topic == null) {
                throw new IllegalArgumentException("required parameter not found: KafkaTopic");
            }
            return format;
        }
    }
}