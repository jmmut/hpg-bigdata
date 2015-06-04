package org.opencb.hpg.bigdata.core.spark;


import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.ga4gh.models.Variant;
import scala.Tuple2;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;


/**
 * Created by jmmut on 2015-05-15.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class SparkHbase {
    static protected Logger logger = Logger.getLogger(SparkHbase.class);

    public void countRows(String[] args) throws IOException {
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
    
    public void mockInsert() throws Exception {

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

        logger.info("testing info log");
        Integer reduce = avroRdd.map((tuple) -> {
            Object datum = tuple._1.datum();
            if (datum instanceof Variant) {
                logger.error("doing insert, datum was a Variant");
                Variant variant = (Variant) datum;
                logger.info("variant.getStart() : " + variant.getStart());
            } else {
                logger.error("datum was NOT a Variant");
            }
            return 1;
        }).reduce(Integer::sum);
        
        logger.error("total variants processed: " + reduce);


        /*
        JavaRDD<String> lines = ctx.textFile(file);
        JavaPairRDD<String, String> keyValue = lines.mapToPair(line -> {
            String[] split = line.split(" ");
            return new Tuple2<>(split[0], split[1]);
        });

        keyValue.saveAsNewAPIHadoopDataset(conf);
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(hTableName);
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
        */
    }

    public void mockWrite() {
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
}
