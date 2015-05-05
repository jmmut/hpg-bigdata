package org.opencb.hpg.bigdata.core.io.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.ga4gh.models.Variant;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by jmmut on 2015-04-30.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class AvroReaderTest {
    
    @Test
    public void indexVariants() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("test"));
        
        byte[] row = Bytes.toBytes("row4");
        Put put = new Put(row);
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("a"), Bytes.toBytes("value4"));
        table.put(put);
        
        table.close();
    }

    private static void readVariants(InputStream inputStream) throws IOException {
        DatumReader<Variant> datumReader = new GenericDatumReader<>(Variant.getClassSchema());
        DataFileStream<Variant> dataFileStream = new DataFileStream<>(inputStream, datumReader);

        int numVariants = 0;
        Schema schema = dataFileStream.getSchema();
        Variant resuse = new Variant();

        while (dataFileStream.hasNext()) {
            dataFileStream.next(resuse);
            numVariants++;
        }
        System.out.println("numVariants = " + numVariants);
    }

}
