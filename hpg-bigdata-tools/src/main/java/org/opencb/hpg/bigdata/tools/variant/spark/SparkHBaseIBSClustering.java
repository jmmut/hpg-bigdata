package org.opencb.hpg.bigdata.tools.variant.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.converter.Converter;
import org.opencb.hpg.bigdata.core.connectors.Connector;

/**
 * Created by jmmut on 2015-12-17.
 *
 * Considerations:
 * * not sure if hardcode to read from hbase, i.e. SparkIBSClustering or SparkHBaseIBSClustering
 *      To make it generic, the easiest way might be to ask directly for a JavaRDD.
 *
 *
 * Algorithm:
 *
 *  foreach partition in hbase
 *      create batch of variants
 *      foreach pair of individuals in batch
 *      accumulate accross all variants the counts
 *      compute distance
 *      store it in hbase
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class SparkHBaseIBSClustering {

    /**
     * TODO jmmut: think about adding a sample set.
     * @param tableName source table
     * @param converter convert type from DB native to workable-with model
     */
    public void calculate(String tableName, Converter<Result, Variant> converter) {

        SparkConf sparkConf = new SparkConf().setAppName("JavaRowCount").setMaster("local[3]");    // 3 threads
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);


        // single place where we hardcode Hbase
        Connector connector;
        Configuration conf = HBaseConfiguration.create();
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        JavaPairRDD<ImmutableBytesWritable, Result> rdd =
                ctx.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        // where is the code of this??
//        rdd.cogroup()

        JavaRDD<Variant> variants = rdd.map(v1 -> converter.convert(v1._2));
        long count = variants.count();
        System.out.println("table " + tableName + " has " + count + " rows");
    }
}
