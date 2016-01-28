package org.opencb.hpg.bigdata.tools.variant.spark;

import org.opencb.biodata.tools.variant.algorithm.IdentityByState;

/**
 * Created by jmmut on 2016-01-28.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public interface PairWriter {
    void writePair(String firstSample, String secondSample, IdentityByState ibs);
}
