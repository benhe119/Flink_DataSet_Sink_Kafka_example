


public static void main(String[] args) throws Exception {

    DataSet ds = new DateSet<Tuple7<String, String, String, String, String, String, Integer>>;
    /* sink to kafka */
    // ds is the DataSet
    ds.output(KafkaOutputFormat.buildKafkaOutputFormat()
            .setBootstrapServers("192.168.100.101:9092")
            .setTopic("testTopic")
            .setAcks("all")
            .setBatchSize("16384")
            .setBufferMemory("33554432")
            .setLingerMs("100")
            .setRetries("2")
            .finish()
    ).setParallelism(parallelism);

    /* execute */
    env.execute("DataSet Sink To Kafka");

}
    

    
