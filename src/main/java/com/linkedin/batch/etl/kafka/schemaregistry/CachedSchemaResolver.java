package com.linkedin.batch.etl.kafka.schemaregistry;

/**
 * Mock implementation of a CachedSchemaResolver as used by the KafkaAvroMessageDecoder
 */
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.linkedin.batch.etl.kafka.mapred.EtlInputFormat;

public class CachedSchemaResolver implements SchemaResolver {

    SchemaRegistryClient registry;
    private Map<String, Schema> cache = new HashMap<String, Schema>();

    // The context is passed to instantiate to get any parameters from the
    // hadoop-conf to create the registry client
    public CachedSchemaResolver(TaskAttemptContext context, String topicName) throws SchemaRegistryException {
        if (registry == null) {
               @SuppressWarnings("rawtypes")
            Constructor constructor = null;
            try {
                constructor = Class.forName(EtlInputFormat.getSchemaRegistryType(context)).getConstructor(new Class[]{JobContext.class});
                registry = (SchemaRegistryClient)constructor.newInstance(context);
            } catch (Exception e)
            {
                throw new SchemaRegistryException("Error in creating the specified schema registry object");
            }
        }
        
    }

    public String resolve(SchemaRegistryClient registry, String topicName) {
        String targetSchema = registry.getLatestSchemaByName(topicName);
        return targetSchema;                                                                           
    }
    
    public Schema resolve(byte[] bs)
    {
        Schema schema = cache.get(bs.toString());
        if (schema != null)
            return schema;
        try {
            String s = registry.getSchemaByID(bs.toString());
            schema = Schema.parse(s);
            cache.put(bs.toString(), schema);
        } catch (Exception e) {
            throw new RuntimeException("Error while resolving schema id:" + bs.toString() + " msg:" + e.getMessage());
        }
        return schema;  
    }

}
