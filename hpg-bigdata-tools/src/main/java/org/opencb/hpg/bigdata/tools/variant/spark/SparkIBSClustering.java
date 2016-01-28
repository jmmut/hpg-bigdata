package org.opencb.hpg.bigdata.tools.variant.spark;

import org.apache.spark.api.java.JavaRDD;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.algorithm.IdentityByState;
import org.opencb.biodata.tools.variant.algorithm.IdentityByStateClustering;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.toIntExact;

/**
 * Created by jmmut on 2016-01-14.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class SparkIBSClustering {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkIBSClustering.class);

    /**
     * generic spark algorithm.
     * autonote: perhaps rdd.cogroup or rdd.cartesian are useful
     * @param variants rdd of variants. May be got from files or from hbase.
     * @param pairWriter output each pair here
     * @throws IOException using outputstreams
     */
    public void calculate(JavaRDD<Variant> variants, PairWriter pairWriter) throws IOException {
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

                pairWriter.writePair(String.valueOf(i), String.valueOf(j), ibs);
            });
        }
    }

    public static void main(String[] args) {

        LOGGER.info("info log: IBS test");
        if (args.length != 1) {
            System.out.println("only 1 argument needed: filename");
            return;
        }

        try {
            // TODO: choose SparkIBSClustering implementation by reflection
            JavaRDD<Variant> variants = new SparkVcfIBSClustering().getRDD(args[0]);

            new SparkIBSClustering().calculate(variants, new SystemOutPairWriter());

        } catch (IOException e) {
            LOGGER.error("IBS failed: ", e);
        }
    }
}
