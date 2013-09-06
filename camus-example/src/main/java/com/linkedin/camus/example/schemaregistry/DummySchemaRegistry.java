package com.linkedin.camus.example.schemaregistry;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import com.linkedin.camus.example.records.DummyLog;
import com.linkedin.camus.example.records.DummyLog2;
import com.linkedin.camus.example.records.page_visit;
import com.linkedin.camus.schemaregistry.MemorySchemaRegistry;

/**
 * This is a little dummy registry that just uses a memory-backed schema
 * registry to store two dummy Avro schemas. You can use this with
 * camus.properties
 */
public class DummySchemaRegistry extends MemorySchemaRegistry<Schema> {
	public DummySchemaRegistry(Configuration conf) {
		super();
                //super.register("page_visits", page_visit.newBuilder().build().getSchema());
		super.register("page_visits", page_visit.SCHEMA$);
                super.register("page_visits1", page_visit.SCHEMA$);
                
                //super.register("DUMMY_LOG", DummyLog.newBuilder().build().getSchema());
		//super.register("DUMMY_LOG_2", DummyLog2.newBuilder().build()
			//	.getSchema());
	}
}
