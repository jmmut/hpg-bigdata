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

package org.opencb.hpg.bigdata.tools.variant.ibs;

import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.algorithm.IdentityByState;
import org.opencb.biodata.tools.variant.algorithm.IdentityByStateClustering;
import org.opencb.hpg.bigdata.core.connectors.Connector;
import org.opencb.hpg.bigdata.tools.spark.SparkToolExecutor;
import org.opencb.hpg.bigdata.tools.spark.datasource.HBaseVariantSparkDataSource;
import org.opencb.hpg.bigdata.tools.spark.datasource.SparkDataSource;
import org.opencb.hpg.bigdata.tools.spark.datasource.VcfSparkDataSource;
import org.opencb.hpg.bigdata.tools.variant.ibs.writers.FileIbsPairWriter;
import org.opencb.hpg.bigdata.tools.variant.ibs.writers.HBaseIbsPairWriter;
import org.opencb.hpg.bigdata.tools.variant.ibs.writers.IbsPairWriter;
import org.opencb.hpg.bigdata.tools.variant.ibs.writers.SystemOutIbsPairWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;

/**
 * Created by jmmut on 2016-01-14.
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
public class SparkIBSClustering extends SparkToolExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkIBSClustering.class);
    public static final String HBASE = "hbase";
    public static final String FILE = "file";
    public static final String STDOUT = "stdout";
    private IbsPairWriter ibsPairWriter;
    private SparkDataSource<Variant> sparkDataSource;

    public SparkIBSClustering(SparkDataSource<Variant> sparkDataSource, IbsPairWriter ibsPairWriter) {
        this.sparkDataSource = sparkDataSource;
        this.ibsPairWriter = ibsPairWriter;
    }

    /**
     * generic spark algorithm.
     * autonote: perhaps rdd.cogroup or rdd.cartesian are useful
     * @throws Exception if the writing fails
     */
    public void execute() throws Exception {

        JavaRDD<Variant> variants = sparkDataSource.createRDD();
        if (!variants.isEmpty()) {
//            List<String> samplesNames = variants.takeSample(true, 1).get(0).getStudies().get(0).getOrderedSamplesName();
            List<List<String>> samplesData = variants.takeSample(true, 1).get(0).getStudies().get(0).getSamplesData();
            int numSamples = samplesData.size();
            IdentityByStateClustering ibsc = new IdentityByStateClustering();
            ibsc.forEachPair(numSamples, (i, j, compound) -> {
//                variants.mapPartitions(variant -> {   // maybe?
                Map<Integer, Long> ibsMap = variants.map(variant -> {

//                    variants.foreachPartition(variantIterator -> {    // Not likely to work
//                    variantIterator.forEachRemaining(variant -> {// Not likely to work
                    StudyEntry studyEntry = variant.getStudies().get(0);
                    Map<String, Integer> formatPositions = studyEntry.getFormatPositions();
                    String gtI = variant.getStudies().get(0).getSamplesData().get(i).get(formatPositions.get("GT"));
                    String gtJ = variant.getStudies().get(0).getSamplesData().get(j).get(formatPositions.get("GT"));
                    Genotype genotypeI = new Genotype(gtI);
                    Genotype genotypeJ = new Genotype(gtJ);

                    // instantiating a new IBSC in order to not serialize the outer one
                    return new IdentityByStateClustering()
                            .countSharedAlleles(genotypeI.getAllelesIdx().length, genotypeI, genotypeJ);
                }).countByValue();
                // here, pair i_j has ibs[] = [x, y, z]

                // convert the map to IdentityByState
                IdentityByState ibs = new IdentityByState();
                ibsMap.entrySet().stream().forEach(entry -> ibs.ibs[entry.getKey()] = toIntExact(entry.getValue()));

                ibsPairWriter.writePair(String.valueOf(i), String.valueOf(j), ibs);
            });
        }
    }

    /**
     * steps:
     * - parameter basic validation
     * - reader/writer implementation choice, based on parameters. reflection if necessary
     * - create spark context
     * - actual computation
     * @param args input, inputType, output, outputType
     * @throws Exception wrong arguments, IOExceptions, etc. We don't constraint the interface
     */
    public static void main(String[] args) throws Exception {

        LOGGER.info("info log: IBS test");

        // basic parameter validation
        if (args.length != 5) {
            throw new Exception("5 argument are required: connector, inputType, input filename, outputType and output");
        }

        String connectorClassName = args[0];
        connectorClassName = "null".equals(connectorClassName) ? null : connectorClassName;
        String inputType = args [1];
        String input = args[2];
        String outputType = args[3];
        String output = args[4];

        if (inputType == null || input == null || outputType == null) {
            throw new Exception("at least 3 argument are required to be non-null: inputType, input filename and outputType");
        }

        if (outputType.equalsIgnoreCase(HBASE) && output == null) {
            throw new Exception("if you want to write the results to hbase, the output tableName is required");
        } else if (outputType.equalsIgnoreCase(FILE) && output == null) {
            throw new Exception("if you want to write the results to a file, the output filePath is required");
        }

        if (inputType.equalsIgnoreCase(HBASE) && connectorClassName == null) {
            throw new Exception(
                    "if you want to read from HBase, you need to provide a Connector className to convert Hbase rows to Variants");
        }


        // spark context
        SparkConf sparkConf = createSparkConf("IbsSparkAnalysis", "local", 2, true);
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);


        // choose input implementation
        SparkDataSource sparkDataSource;

        if (inputType.equalsIgnoreCase(HBASE)) {
            Class clazz = Class.forName(connectorClassName);
            Connector<Result, Variant> connector = (Connector) clazz.getConstructor(String.class).newInstance(input);
            sparkDataSource = new HBaseVariantSparkDataSource(sparkConf, ctx, input, connector.getConverter());
        } else
            if (inputType.equalsIgnoreCase(FILE)) {
            sparkDataSource = new VcfSparkDataSource(sparkConf, ctx, Paths.get(input));
        } else {
            throw new IllegalArgumentException(String.format(
                    "don't know how to read from %s, try %s or %s", inputType, HBASE, FILE));
        }

        // choose output type implementation
        IbsPairWriter ibsPairWriter;
        if (outputType.equalsIgnoreCase(HBASE)) {
            ibsPairWriter = new HBaseIbsPairWriter(output);

        } else if (outputType.equalsIgnoreCase(FILE)) {
            ibsPairWriter = new FileIbsPairWriter(output);

        } else if (outputType.equalsIgnoreCase(STDOUT)) {
            ibsPairWriter = new SystemOutIbsPairWriter();

        } else {
            throw new IllegalArgumentException(String.format(
                    "don't know how to write in %s, try %s, %s or %s", outputType, STDOUT, HBASE, FILE));
        }


        // do the actual computation, once we know how to read and write
        new SparkIBSClustering(sparkDataSource, ibsPairWriter).execute();
        ibsPairWriter.close();
    }
}
