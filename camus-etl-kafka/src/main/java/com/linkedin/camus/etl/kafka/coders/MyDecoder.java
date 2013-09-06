/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.example.records.page_visit;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.message.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 *
 * @author ballarde
 */
public class MyDecoder extends MessageDecoder<byte[], Record> {

    protected DecoderFactory decoderFactory;
    
    @Override
    public CamusWrapper<Record> decode(byte[] payload) {
       try
		{ 
                    System.out.println("============ " + payload);
			
			
	//		Schema schema = super.registry.getLatestSchemaByTopic(super.topicName).getSchema();
			
          //              System.out.println("================ " + schema.toString());
                        
                        
	//		reader.setSchema(schema);
	
                        //Schema schema = page_visit.newBuilder().build().getSchema();
                       Schema.Parser parser = new Schema.Parser();
                       // Schema schema = page_visit.SCHEMA$;
                        Path inFile = new Path("avro/page_visit.avsc");
                        
                        Configuration conf = new Configuration();
                        FileSystem fs = FileSystem.get(conf);
                        FSDataInputStream in = fs.open(inFile);
                        
                        Schema schema = parser.parse(in);
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
                        //return new CamusWrapper<Record> (reader.read(null, decoder));
                    
                        return new CamusAvroWrapper (reader.read(null, decoder));
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

    public static class CamusAvroWrapper extends CamusWrapper<Record> {

	    public CamusAvroWrapper(Record record) {
            super(record);
            Record header = (Record) super.getRecord().get("header");
   	        if (header != null) {
               if (header.get("server") != null) {
                   put(new Text("server"), new Text(header.get("server").toString()));
               }
               if (header.get("service") != null) {
                   put(new Text("service"), new Text(header.get("service").toString()));
               }
            }
        }
	    
	    @Override
	    public long getTimestamp() {
	        Record header = (Record) super.getRecord().get("header");

	        if (header != null && header.get("time") != null) {
	            return (Long) header.get("time");
	        } else if (super.getRecord().get("timestamp") != null) {
       
                        
                        //Utf8 time = (Utf8)super.getRecord().get("timestamp");
                        
                       // if(time.toString().isEmpty())
                            return System.currentTimeMillis();
                       // return (Long) Long.getLong(time.toString());
         
	        } else {
	            return System.currentTimeMillis();
	        }
	    }
	}
    
}
