package com.linkedin.batch.etl.kafka.schemaregistry;

import org.apache.avro.Schema;

public interface SchemaResolver {

    public String resolve(SchemaRegistryClient registry, String topicName);
    public Schema resolve(byte[] bs);
    
    
}
