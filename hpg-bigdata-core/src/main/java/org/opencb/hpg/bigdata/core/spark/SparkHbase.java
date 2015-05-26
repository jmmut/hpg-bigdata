package org.opencb.hpg.bigdata.core.spark;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.PairRDDFunctions;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
