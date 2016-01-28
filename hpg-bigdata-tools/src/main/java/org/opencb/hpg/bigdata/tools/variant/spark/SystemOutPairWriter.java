package org.opencb.hpg.bigdata.tools.variant.spark;

import org.opencb.biodata.tools.variant.algorithm.IdentityByState;
import org.opencb.biodata.tools.variant.algorithm.IdentityByStateClustering;

/**
 * Created by jmmut on 2016-01-28.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class SystemOutPairWriter implements PairWriter {

    public SystemOutPairWriter() {
        System.out.println("ibs result header: IID1\tIID2\tDST\tZ0\tZ1\tZ2\n");
    }

    @Override
    public void writePair(String firstSample, String secondSample, IdentityByState ibs) {
        String line = new IdentityByStateClustering().pairToString(firstSample, secondSample, ibs);
        System.out.println("ibs result: " + line);
    }
}
