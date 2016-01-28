package org.opencb.hpg.bigdata.tools.variant.spark;

import org.apache.spark.api.java.JavaRDD;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.algorithm.IdentityByState;
import org.opencb.biodata.tools.variant.algorithm.IdentityByStateClustering;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;

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
     * @throws IOException using outputstreams
     */
    public void calculate(JavaRDD<Variant> variants) throws IOException {
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
                    int countSharedAlleles = new IdentityByStateClustering()
                            .countSharedAlleles(genotypeI.getAllelesIdx().length, genotypeI, genotypeJ);
                    System.out.println("countSharedAlleles = " + countSharedAlleles + "; genotypes: {"
                            + genotypeI.toString() + ", " + genotypeJ.toString() + "}");
                    return countSharedAlleles;
                }).countByValue();
                // here, pair i_j has ibs[] = [x, y, z]

                // convert the map to IdentityByState
                IdentityByState ibs = new IdentityByState();
                ibsMap.entrySet().stream().forEach(entry -> ibs.ibs[entry.getKey()] = toIntExact(entry.getValue()));

                System.out.println("ibsMap = " + ibsMap.toString());
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                OutputStreamWriter osw = new OutputStreamWriter(baos);
                for (int k = 0; k < 3; k++) {
                    System.out.println("ibs.ibs[" + k + "] = " + ibs.ibs[k]);
                }
                new IdentityByStateClustering().writePair(osw, String.valueOf(i), String.valueOf(j), ibs);
                osw.close();
                System.out.println("osw.toString() = " + osw.toString());
                String line = new String(baos.toByteArray());
                System.out.println("pair [" + i + ", " + j + "] has ibs " + line);
            });
        }
    }
}
