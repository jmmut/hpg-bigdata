package org.opencb.hpg.bigdata.tools.variant.spark;

import org.apache.spark.api.java.JavaRDD;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.algorithm.IdentityByStateClustering;

import java.util.List;
import java.util.Map;

/**
 * Created by jmmut on 2016-01-14.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class SparkIBSClustering {

    /**
     * generic spark algorithm.
     * autonote: perhaps rdd.cogroup or rdd.cartesian are useful
     * @param variants rdd of variants. May be got from files or from hbase.
     */
    public void calculate(JavaRDD<Variant> variants) {
//        long count = variants.count();
//        System.out.println("there are " + count + " rows");
        if (!variants.isEmpty()) {
            List<List<String>> samplesData = variants.takeSample(true, 1).get(0).getStudies().get(0).getSamplesData();
            int numSamples = samplesData.size();
//            List<String> samplesNames = variants.takeSample(true, 1).get(0).getStudies().get(0).getOrderedSamplesName();
//            int numSamples = samplesNames.size();
            IdentityByStateClustering ibsc = new IdentityByStateClustering();
            int studyIndex = 0;
            ibsc.forEachPair(numSamples, (i, j, compound) -> {
//                variants.mapPartitions(variant -> {   // maybe?
                Map<Integer, Long> ibs = variants.map(variant -> {

//                    variants.foreachPartition(variantIterator -> {    // Not likely to work
//                    variantIterator.forEachRemaining(variant -> {// Not likely to work
                    StudyEntry studyEntry = variant.getStudies().get(studyIndex);
                    Map<String, Integer> formatPositions = studyEntry.getFormatPositions();
                    String gtI = samplesData.get(i).get(formatPositions.get("GT"));
                    String gtJ = samplesData.get(i).get(formatPositions.get("GT"));
                    Genotype genotypeI = new Genotype(gtI);
                    Genotype genotypeJ = new Genotype(gtJ);

                    return ibsc.countSharedAlleles(genotypeI.getAllelesIdx().length, genotypeI, genotypeJ);
                }).countByValue();
                // here, pair i_j has ibs[] = [x, y, z]
                System.out.println("pair [" + i + ", " + j + "] has ibs " + ibs.toString());
            });
        }
    }
}
