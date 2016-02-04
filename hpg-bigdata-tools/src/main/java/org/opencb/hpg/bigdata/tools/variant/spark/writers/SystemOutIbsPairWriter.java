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

package org.opencb.hpg.bigdata.tools.variant.spark.writers;

import org.opencb.biodata.tools.variant.algorithm.IdentityByState;
import org.opencb.biodata.tools.variant.algorithm.IdentityByStateClustering;

/**
 * Created by jmmut on 2016-01-28.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class SystemOutIbsPairWriter implements IbsPairWriter {

    public SystemOutIbsPairWriter() {
        System.out.println("ibs result header: IID1\tIID2\tDST\tZ0\tZ1\tZ2\n");
    }

    @Override
    public void writePair(String firstSample, String secondSample, IdentityByState ibs) {
        String line = new IdentityByStateClustering().pairToString(firstSample, secondSample, ibs);
        System.out.println("ibs result: " + line);
    }

    @Override
    public void close() throws Exception {
    }
}
