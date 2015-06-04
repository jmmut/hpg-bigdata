package org.opencb.hpg.bigdata.core.spark;


import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.ga4gh.models.Call;
import org.ga4gh.models.Variant;
import scala.Tuple2;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Created by jmmut on 2015-05-15.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class SparkHbase {
    static protected Logger logger = Logger.getLogger(SparkHbase.class);

    public void sparkHbaseReadRows(String[] args) throws IOException {
        String tableName = "test";
        if (args.length != 1) {
            logger.warn("expected 1 argument (table name), using 'test'");
        } else {
            tableName = args[0];
        }
        Configuration conf = HBaseConfiguration.create();

        SparkConf sparkConf = new SparkConf().setAppName("JavaRowCount").setMaster("local[3]");    // 3 threads
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        JavaPairRDD<ImmutableBytesWritable, Result> rdd = 
                ctx.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaRDD<Integer> map = rdd.map(v1 -> 1);
        Integer reduce = map.reduce((v1, v2) -> v1 + v2);
        System.out.println("table " + tableName + " has " + reduce + " rows");
    }
    
    public void avroToSpark() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        String tableName = "testVariants";
        TableName hTableName = TableName.valueOf(tableName);
        InputStream inputStream;
        String input = "/home/jmmut/appl/hpg-bigdata/resources/small.vcf.avro.snz";
        try {
            inputStream = new FileInputStream(input);
        } catch (FileNotFoundException e) {
            logger.error("cannot make test without file");
            logger.error(e.toString());
            return;
        }

//        AvroKeyInputFormat:49 - Reader schema was not set. Use AvroJob.setInputKeySchema() if desired.
        SparkConf sparkConf = new SparkConf().setAppName("JavaAvroSpark").setMaster("local[3]");    // 3 threads
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);

        conf.set(AvroKeyInputFormat.INPUT_DIR, input);
        JavaPairRDD<AvroKey, NullWritable> avroRdd = ctx.newAPIHadoopRDD(conf,
                AvroKeyInputFormat.class,
                AvroKey.class,
                NullWritable.class);

        Integer reduce = avroRdd.map((tuple) -> {
            Object datum = tuple._1.datum();
            if (datum instanceof Variant) {
                logger.info("doing insert, datum was a Variant");
                Variant variant = (Variant) datum;
                logger.info("variant.getStart() : " + variant.getStart());
//                logger.info("variant.getReferenceName() : " + variant.getReferenceName());
                return 1;
            } else {
                logger.info("datum was NOT a Variant");
                return 10000;
            }
        }).reduce(Integer::sum);

        logger.error("total variants processed: " + reduce);
    }


    public void avroToSparkToHbase() throws Exception {

        String tableName = "testVariants2";
        InputStream inputStream;
        String input = "/home/jmmut/appl/hpg-bigdata/resources/small.vcf.avro.snz";
        try {
            inputStream = new FileInputStream(input);
        } catch (FileNotFoundException e) {
            logger.error("cannot make test without file");
            logger.error(e.toString());
            return;
        }

//        AvroKeyInputFormat:49 - Reader schema was not set. Use AvroJob.setInputKeySchema() if desired.
        SparkConf sparkConf = new SparkConf().setAppName("JavaAvroSpark").setMaster("local[3]");    // 3 threads
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        
        Configuration conf = HBaseConfiguration.create();
        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        
        conf.set(AvroKeyInputFormat.INPUT_DIR, input);
        JavaPairRDD<AvroKey, NullWritable> avroRdd = ctx.newAPIHadoopRDD(conf,
                AvroKeyInputFormat.class,
                AvroKey.class,
                NullWritable.class);

        Integer reduce = avroRdd.map((tuple) -> {
            Object datum = tuple._1.datum();
            if (datum instanceof Variant) {
                logger.info("doing insert, datum was a Variant");
                Variant variant = (Variant) datum;
                logger.info("variant.getStart() : " + variant.getStart());
//                Configuration localConf = HBaseConfiguration.create();
//                localConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
                Connection connection = ConnectionFactory.createConnection();
//                Connection connection = ConnectionFactory.createConnection(localConf);
                TableName hTableName = TableName.valueOf(tableName);
                Table table = connection.getTable(hTableName);
                Put put;
                byte[] row = Bytes.toBytes(String.format("%s_%08d_%s_%s",
                        variant.getReferenceName(),
                        variant.getStart(),
                        variant.getReferenceBases(),
                        variant.getAlternateBases().isEmpty() ? "" : variant.getAlternateBases().get(0)));
                put = new Put(row);
                try {
                    variant.setCalls(new ArrayList<>());
                    String toJson = variant.toString();
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("c"), Bytes.toBytes(toJson));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                table.put(put);
                table.close();
                return 1;
            } else {
                logger.info("datum was NOT a Variant");
                return 10000;
            }
        }).reduce(Integer::sum);

        logger.error("total variants processed: " + reduce);
    }

    public void sparkHadoopFileWrite() {
        Configuration conf = HBaseConfiguration.create();
        String tableName = "putexample";

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[3]");    // 3 threads
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//        conf.set
        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        conf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp/sparkhbaseoutputdir");

        String file = "/tmp/key-value.txt";
        JavaRDD<String> lines = ctx.textFile(file);
        JavaPairRDD<String, String> keyValue = lines.mapToPair(line -> {
            String[] split = line.split(" ");
            return new Tuple2<>(split[0], split[1]);
        });

        keyValue.saveAsNewAPIHadoopDataset(conf);
    }
    
    public void mockHbaseWrite() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("test3"));

        byte[] row = Bytes.toBytes("row" + System.currentTimeMillis());
        Put put = new Put(row);
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("a"), Bytes.toBytes("value4"));
        table.put(put);
        logger.info("made mock insert");

        table.close();
    }
}
