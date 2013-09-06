/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.example.records.page_visit;
import com.linkedin.camus.example.schemaregistry.DummySchemaRegistry;
import com.linkedin.camus.schemaregistry.CachedSchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import java.io.File;
import java.util.Properties;
import kafka.message.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

/**
 *
 * @author ballarde
 */
public class MyDecoder1 extends MessageDecoder<byte[], Record>{

    protected DecoderFactory decoderFactory;
    protected SchemaRegistry<Schema> registry;
    private Schema latestSchema;
    public static final String KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS = "kafka.message.coder.schema.registry.class";
    	
    @Override
	public void init(Properties props, String topicName) {
	    super.init(props, topicName);
	   
            try {
            SchemaRegistry<Schema> registry = (SchemaRegistry<Schema>) Class
                    .forName(
                           // props.getProperty(KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS)).newInstance();
                    "DummySchemaRegistry").newInstance();
                    
            registry.init(props);
            
            this.registry = new CachedSchemaRegistry<Schema>(registry);
            this.latestSchema = registry.getLatestSchemaByTopic(topicName).getSchema();
        } catch (Exception e) {
            throw new MessageDecoderException(e);
        }

       // decoderFactory = DecoderFactory.get();
	}
    
    
    
    @Override
    public CamusWrapper<Record> decode(byte[] payload) {
       try
		{ 
                    System.out.println("============ " + payload);
			
			//Schema schema = registry.getLatestSchemaByTopic(super.topicName).getSchema();
			
          //              System.out.println("================ " + schema.toString());
                        
                        
	//		reader.setSchema(schema);
	
                        //Schema schema = page_visit.newBuilder().build().getSchema();
                       //Schema.Parser parser = new Schema.Parser();
                       Schema schema = page_visit.SCHEMA$;
                       // Path inFile = new Path("avro/page_visit.avsc");
                        
                        //Configuration conf = new Configuration();
                        //FileSystem fs = FileSystem.get(conf);
                        //FSDataInputStream in = fs.open(inFile);
                        
                        //Schema schema = parser.parse(in);
                        //Schema schema = parser.parse(new File("page_visit.avsc"));
                        
                        GenericDatumReader<Record> reader = new GenericDatumReader<Record>(schema);
                        
                        System.out.println("================ " + schema.toString());
                        
                        System.out.println("============ " + payload);
                        
                        
                        //return null;
			/*return new CamusWrapper<Record>(reader.read(null,                
                    //decoderFactory.binaryDecoder(payload, null)
                    
                    decoderFactory.jsonDecoder(
                            schema, 
                            new String(
                                    payload, 
                                    //Message.payloadOffset(message.magic()),
                                    Message.MagicOffset(),
                                    payload.length - Message.MagicOffset()
                            )
                    )
            )
                                );*/
                        
                        Decoder decoder = decoderFactory.get().binaryDecoder(payload, null);
                        return new CamusWrapper<Record> (reader.read(null, decoder));
                    
                        /*
                   return new CamusWrapper<Record>(reader.read(
                    null, 
                    decoderFactory.get().jsonDecoder(
                            schema, 
                            new String(
                                    payload, 
                                    //Message.payloadOffset(message.magic()),
                                    Message.MagicOffset(),
                                    payload.length - Message.MagicOffset()
                            )
                    )
            ));*/
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
        
        
        //return new CamusWrapper<Text>(new Text(message));
    }

}
