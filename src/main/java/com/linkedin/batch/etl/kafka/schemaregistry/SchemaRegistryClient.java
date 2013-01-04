package com.linkedin.batch.etl.kafka.schemaregistry;

import org.apache.hadoop.mapred.JobContext;

public interface SchemaRegistryClient {
	
    public SchemaRegistryClient getInstance(JobContext context);
    
	public String register(String schema) throws SchemaRegistryException;
	
	public String lookUp(String schema);
	
	public SchemaDetails lookUpLatest(String topic);
	
	public String getSchemaByID(String Id) throws SchemaNotFoundException;
	
	public String getLatestSchemaByName(String topicName);
		

}