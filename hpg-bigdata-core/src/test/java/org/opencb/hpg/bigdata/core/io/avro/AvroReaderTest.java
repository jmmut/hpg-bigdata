package org.opencb.hpg.bigdata.core.io.avro;

import static junit.framework.TestCase.*;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.ga4gh.models.Variant;
import org.junit.Test;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Created by jmmut on 2015-04-30.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class AvroReaderTest {
    protected Logger logger = Logger.getLogger(this.getClass());
    
    class VariantHbaseTask implements ParallelTaskRunner.Task<Variant, Variant> {
        protected Logger logger = Logger.getLogger(this.getClass());
        Configuration conf;
        private final TableName tableName;

        public VariantHbaseTask(Configuration conf, TableName tableName) {
            this.tableName = tableName;
            this.conf = conf;
        }

        @Override
        public List<Variant> apply(List<Variant> variants) {
            try {
                Connection connection = ConnectionFactory.createConnection(conf);
                Table table = connection.getTable(tableName);
                List<Put> puts = new ArrayList<>(variants.size());
                Put put;
                for (Variant variant : variants) {
                    byte[] row = Bytes.toBytes(String.format("%s_%08d_%s_%s", 
                            variant.getReferenceName(), 
                            variant.getStart(), 
                            variant.getReferenceBases(), 
                            variant.getAlternateBases().isEmpty()? "" : variant.getAlternateBases().get(0)));
                    put = new Put(row);
                    try {
                        String toJson = variant.toString();
                        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("c"), Bytes.toBytes(toJson));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    puts.add(put);
                }
                table.put(puts);
                table.close();
                logger.info("another batch of " + puts.size() + " elements has been put");
            } catch (IOException e) {
                logger.error(e.toString());
            }
            return null;
        }
    }
    
    @Test
    public void indexVariants() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("testVariants");
        try {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (TableNotFoundException e) {}   // no table to delete, this is OK
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        descriptor.addFamily(new HColumnDescriptor("cf"));
        admin.createTable(descriptor);
        admin.close();
        InputStream inputStream;
        try {
            inputStream = new FileInputStream("../resources/small.vcf.avro.snz");
        } catch (FileNotFoundException e) {
            logger.error("cannot make test without file");
            logger.error(e.toString());
            fail();
            return;
        }
        DataReader<Variant> reader = new AvroReader<>(inputStream);
        Supplier<ParallelTaskRunner.Task<Variant, Variant>> taskSupplier = () -> new VariantHbaseTask(conf, tableName);
        
        int numTasks = 2;
        int batchSize = 100;
        int capacity = batchSize * 2;
        boolean sorted = false;
        ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numTasks, batchSize, capacity, sorted);
        ParallelTaskRunner<Variant, Variant> runner = new ParallelTaskRunner<>(
                reader, taskSupplier, null, config);
        
        runner.run();
    }
    
    @Test
    public void mockIndex() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("test"));

        byte[] row = Bytes.toBytes("row" + System.currentTimeMillis());
        Put put = new Put(row);
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("a"), Bytes.toBytes("value4"));
        table.put(put);
        logger.info("made mock insert");
        
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
