package com.linkedin.camus.etl.kafka.coders;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.example.records.page_visit;

public class LatestSchemaKafkaAvroMessageDecoder extends KafkaAvroMessageDecoder
{

	@Override
	public CamusWrapper<Record> decode(byte[] payload)
	{
		try
		{
			GenericDatumReader<Record> reader = new GenericDatumReader<Record>();
			
			Schema schema = super.registry.getLatestSchemaByTopic(super.topicName).getSchema();
			
          //              System.out.println("================ " + schema.toString());
                        
                        
			reader.setSchema(schema);
	
                        //Schema schema = page_visit.newBuilder().build().getSchema();
                        
			return new CamusWrapper<Record>(reader.read(
                    null, 
                    decoderFactory.jsonDecoder(
                            schema, 
                            new String(
                                    payload, 
                                    //Message.payloadOffset(message.magic()),
                                    Message.MagicOffset(),
                                    payload.length - Message.MagicOffset()
                            )
                    )
            ));
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
}