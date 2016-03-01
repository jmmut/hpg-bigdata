package org.opencb.hpg.bigdata.tools.variant.spark;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by jmmut on 2016-03-01.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class SparkIBSClusteringTest {

    @Test
    public void testMain() throws Exception {
        String path = SparkIBSClusteringTest.class.getClassLoader().getResource("ibs.vcf").getPath();

        SparkIBSClustering.main(new String[]{"file", path, "stdout"});

    }
}