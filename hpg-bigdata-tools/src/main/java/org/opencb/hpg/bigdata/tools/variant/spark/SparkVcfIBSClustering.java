package org.opencb.hpg.bigdata.tools.variant.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantVcfFactory;
import org.opencb.biodata.models.variant.exceptions.NotAVariantException;

import java.util.List;

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
public class SparkVcfIBSClustering {

    /**
     * TODO jmmut: think about adding a sample set.
     * @param filename vcf
     */
    public void calculate(String filename) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaRowCount").setMaster("local[3]");    // 3 threads
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> file = ctx.textFile(filename);
//        VariantVcfReader reader = new VariantVcfReader(variantSource, )
        JavaRDD<Variant> variants = file
                .filter(line -> !line.startsWith("#") && !line.trim().isEmpty())
                .flatMap(line -> {
                    VariantSource variantSource = new VariantSource(filename, "5", "7", "studyName");
                    VariantVcfFactory factory = new VariantVcfFactory();
                    List<Variant> parsed = null;
                    try {
                        parsed = factory.create(variantSource, line);
                    } catch (IllegalArgumentException e) {
                        System.out.println(line);
                        e.printStackTrace();
                        throw e;
                    } catch (NotAVariantException e) {
                        e.printStackTrace();
                        throw e;
                    }

                    return parsed;
                });

        new SparkIBSClustering().calculate(variants);
    }
}
