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

package org.opencb.hpg.bigdata.tools.variant.spark;

import org.apache.hadoop.hbase.client.Result;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.algorithm.IdentityByState;
import org.opencb.biodata.tools.variant.algorithm.IdentityByStateClustering;
import org.opencb.biodata.tools.variant.converter.Converter;
import org.opencb.hpg.bigdata.core.connectors.Connector;
import org.opencb.hpg.bigdata.core.connectors.VcfToVariantConnector;
import org.opencb.hpg.bigdata.tools.variant.spark.adaptors.HBaseVariantRddAdaptor;
import org.opencb.hpg.bigdata.tools.variant.spark.adaptors.VariantRddAdaptor;
import org.opencb.hpg.bigdata.tools.variant.spark.adaptors.VcfVariantRddAdaptor;
import org.opencb.hpg.bigdata.tools.variant.spark.writers.FileIbsPairWriter;
import org.opencb.hpg.bigdata.tools.variant.spark.writers.HBaseIbsPairWriter;
import org.opencb.hpg.bigdata.tools.variant.spark.writers.IbsPairWriter;
import org.opencb.hpg.bigdata.tools.variant.spark.writers.SystemOutIbsPairWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
public class SparkIBSClustering {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkIBSClustering.class);
    public static final String HBASE = "hbase";
    public static final String FILE = "file";
    public static final String STDOUT = "stdout";

    /**
     * generic spark algorithm.
     * autonote: perhaps rdd.cogroup or rdd.cartesian are useful
     * @param variants rdd of variants. May be got from files or from hbase.
     * @param ibsPairWriter output each pair here
     * @throws IOException if the writing fails
     */
    public void calculate(JavaRDD<Variant> variants, IbsPairWriter ibsPairWriter) throws IOException {

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
        String inputType = null;
        String input = null;
        String connectorClassName = null;
        String outputType = null;
        String output = null;

        // basic parameter validation
        if (args.length != 3 && args.length != 4) {
            throw new Exception("at least 3 argument are required: inputType, input filename and outputType");
        }

        inputType = args[0];
        input = args[1];
        outputType = args[2];
        if (inputType == null || input == null || outputType == null) {
            throw new Exception("at least 3 argument are required to be non-null: inputType, input filename and outputType");
        }

        if (args.length == 4) {
            output = args[3];
        }

        if (outputType.equalsIgnoreCase(HBASE) && output == null) {
            throw new Exception("if you want to write the results to hbase, the tableName is required");
        } else if (outputType.equalsIgnoreCase(FILE) && output == null) {
            throw new Exception("if you want to write the results to a file, the filePath is required");
        }

        // choose input implementation
        VariantRddAdaptor rddAdaptor;
        if (inputType.equalsIgnoreCase(HBASE)) {
            Class clazz = Class.forName(connectorClassName);
            Connector<Result, Variant> connector = (Connector) clazz.getConstructor(String.class).newInstance(input);
            rddAdaptor = new HBaseVariantRddAdaptor(input, connector.getConverter());

        } else if (inputType.equalsIgnoreCase(FILE)) {
            Converter<String, List<Variant>> converter = new VcfToVariantConnector(
                    new VariantSource(input, "testFileId", "testStudyId", "testStudyName")).getConverter();
            rddAdaptor = new VcfVariantRddAdaptor(input, converter);

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

        // spark context
        SparkConf sparkConf = new SparkConf().setAppName("IbsSparkAnalysis").setMaster("local[3]");    // 3 threads
        sparkConf.registerKryoClasses(new Class[]{VariantAvro.class});
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);


        // do the actual computation, once we know how to read and write
        JavaRDD<Variant> variants = rddAdaptor.getRdd(ctx);
        new SparkIBSClustering().calculate(variants, ibsPairWriter);
        ibsPairWriter.close();

    }
}
