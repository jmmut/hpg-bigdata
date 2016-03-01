package org.opencb.hpg.bigdata.tools.variant.spark;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by jmmut on 2016-03-01.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class SparkIBSClusteringTest {

    @Test
    public void testLocalfile() throws Exception {
        String path = SparkIBSClusteringTest.class.getClassLoader().getResource("ibs.vcf").getPath();

        SparkIBSClustering.main(new String[]{null, "file", path, "stdout", null});
    }

    /**
     * This test will fail because no Connector class is provided, but here it is shown the usage.
     * @throws Exception of no Connector provided
     */
    @Test
    @Ignore
    public void testHbaseTable() throws Exception {
        String path = SparkIBSClusteringTest.class.getClassLoader().getResource("ibs.vcf").getPath();

        SparkIBSClustering.main(new String[]{null, "hbase", path, "stdout", "/tmp/SparkIBSClusteringTest.txt"});
    }
}