package org.opencb.hpg.bigdata.core.spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;  

/**
 * Created by jmmut on 2015-05-15.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class Wordcount {

    protected Logger logger = Logger.getLogger(this.getClass());
    
    public void sparkAvro(String file, String contains) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[3]");    // 3 threads
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = ctx.textFile(file)
                .filter(s -> s.contains(contains));
        long numMatches = lines.count();
        System.out.println(String.format("sout: numMatches(%s) = %d", contains, numMatches));
        logger.info(String.format("logger: numMatches(%s) = %d", contains, numMatches));
    }
}
