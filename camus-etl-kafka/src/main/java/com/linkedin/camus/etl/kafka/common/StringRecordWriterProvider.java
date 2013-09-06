/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;

import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 *
 * @author ballarde
 */
public class StringRecordWriterProvider implements RecordWriterProvider {

    public String getFilenameExtension() {
        return (".txt");
    }

    public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName, CamusWrapper data, FileOutputCommitter committer) throws IOException, InterruptedException {

        Path path = committer.getWorkPath();
        path = new Path(path, EtlMultiOutputFormat.getUniqueFile(context, fileName, ".txt"));
        final FSDataOutputStream out = path.getFileSystem(context.getConfiguration()).create(path);



        return new RecordWriter<IEtlKey, CamusWrapper>() {
            /**
             * Write the object to the byte stream, handling Text as a special
             * case.
             *
             * @param o the object to print
             * @throws IOException if the write throws, we pass it on
             */
            private void writeObject(Object o) throws IOException {
                if (o instanceof Text) {
                    Text to = (Text) o;
                    out.write(to.getBytes(), 0, to.getLength());
                } else {
                    out.write(o.toString().getBytes("UTF-8"));
                }
            }

            @Override
            public void write(IEtlKey ignore, CamusWrapper data) throws IOException {
                Writable value = (Writable) data.getRecord();

                boolean nullValue = value == null || value instanceof NullWritable;
                if (!nullValue) {
                    writeObject(value);
                }
                out.write('\n');
            }

            @Override
            public synchronized void close(TaskAttemptContext arg0) throws IOException {
                out.close();
            }
        };
    }
}
