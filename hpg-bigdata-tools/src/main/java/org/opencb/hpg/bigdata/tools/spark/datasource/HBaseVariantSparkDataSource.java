/*
 * Copyright 2016 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.hpg.bigdata.tools.spark.datasource;

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

import java.io.IOException;

/**
 * Created by jmmut on 2015-12-17.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class HBaseVariantSparkDataSource extends SparkDataSource<Variant> {

    private String tableName;
    private Converter<Result, Variant> converter;

    /**
     * @param sparkConf spark configuration
     * @param sparkContext spark context
     * @param tableName source table
     * @param converter convert type from DB native to workable-with model
     */
    public HBaseVariantSparkDataSource(SparkConf sparkConf,
                                       JavaSparkContext sparkContext,
                                       String tableName,
                                       Converter<Result, Variant> converter) {
        super(sparkConf, sparkContext);
        this.tableName = tableName;
        this.converter = converter;
    }

    @Override
    public JavaRDD<Variant> createRDD() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        JavaPairRDD<ImmutableBytesWritable, Result> rdd =
                sparkContext.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        // where is the code of this??
//        rdd.cogroup()

        return rdd.map(v1 -> converter.convert(v1._2));
//        new SparkIBSClustering().calculate(variants, new HBasePairWriter(ctx));
    }

}
