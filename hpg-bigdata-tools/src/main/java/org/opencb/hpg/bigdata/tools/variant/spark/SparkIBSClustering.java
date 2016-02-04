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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.algorithm.IdentityByState;
import org.opencb.biodata.tools.variant.algorithm.IdentityByStateClustering;
import org.opencb.hpg.bigdata.tools.variant.spark.adaptors.VcfVariantRddAdaptor;
import org.opencb.hpg.bigdata.tools.variant.spark.writers.FileIbsPairWriter;
import org.opencb.hpg.bigdata.tools.variant.spark.writers.IbsPairWriter;
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

    /**
     * generic spark algorithm.
     * autonote: perhaps rdd.cogroup or rdd.cartesian are useful
     * @param variants rdd of variants. May be got from files or from hbase.
     * @param ibsPairWriter output each pair here
     * @throws IOException if the writing fails
     */
    public void calculate(JavaRDD<Variant> variants, IbsPairWriter ibsPairWriter) throws IOException {
//        long count = variants.count();
//        System.out.println("there are " + count + " rows");

        if (!variants.isEmpty()) {
            List<List<String>> samplesData = variants.takeSample(true, 1).get(0).getStudies().get(0).getSamplesData();
            int numSamples = samplesData.size();
//            List<String> samplesNames = variants.takeSample(true, 1).get(0).getStudies().get(0).getOrderedSamplesName();
//            int numSamples = samplesNames.size();
            IdentityByStateClustering ibsc = new IdentityByStateClustering();
//            System.out.println("before loops");
            ibsc.forEachPair(numSamples, (i, j, compound) -> {
//                variants.mapPartitions(variant -> {   // maybe?
//                System.out.println("starting pair" + i + ", " + j);
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

    public static void main(String[] args) throws IOException {

        LOGGER.info("info log: IBS test");
        if (args.length != 1) {
            System.out.println("only 1 argument needed: filename");
            return;
        }

        SparkConf sparkConf = new SparkConf().setAppName("IbsSparkAnalysis").setMaster("local[3]");    // 3 threads
        sparkConf.registerKryoClasses(new Class[]{VariantAvro.class});
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        // TODO: choose SparkIBSClustering implementation by reflection
        JavaRDD<Variant> variants = new VcfVariantRddAdaptor(args[0]).getRdd(ctx);

//            new SparkIBSClustering().calculate(variants, new HBasePairWriter());
        new SparkIBSClustering().calculate(variants, new FileIbsPairWriter("/tmp/sparklog" + System.currentTimeMillis() + ".txt"));
    }
}
