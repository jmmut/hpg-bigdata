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

package org.opencb.hpg.bigdata.tools.variant.ibs.writers;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.tools.variant.algorithm.IdentityByState;
import org.opencb.biodata.tools.variant.algorithm.IdentityByStateClustering;

import java.io.IOException;

/**
 * Created by jmmut on 2016-01-28.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class HBaseIbsPairWriter implements IbsPairWriter {

    private final Table table;

    /**
     * constructor. As IbsPairWriter is Autocloseable, it is needed to use a try-with-resources
     * when you create a HBaseIbsPairWriter. This way it will auto close its resources properly.
     *
     * Alternatively,  you can call close() yourself to avoid using a try-with-resources.
     *
     * @param tableName for the output
     * @throws IOException if unable to open table
     */
    public HBaseIbsPairWriter(String tableName) throws IOException {

        if (tableName == null) {
            throw new IOException("tableName for HBase not specified, cannot write results");
        }

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "who1");
        conf.set("hbase.master", "who1:60000");

        Connection connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TableName.valueOf(tableName));
    }

    @Override
    public void writePair(String firstSample, String secondSample, IdentityByState ibs) throws IOException {
        byte[] rowBytes = Bytes.toBytes(firstSample + "_" + secondSample);
        String columnFamily = "i"; //ibs
        String columnZ0 = "z0";
        String columnZ1 = "z1";
        String columnZ2 = "z2";
        String columnDistance = "d";
        double distance = new IdentityByStateClustering().getDistance(ibs);


        Put put = new Put(rowBytes);
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnZ0), Bytes.toBytes(ibs.ibs[0]));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnZ1), Bytes.toBytes(ibs.ibs[1]));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnZ2), Bytes.toBytes(ibs.ibs[2]));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnDistance), Bytes.toBytes(distance));

        table.put(put);
    }

    public void close() throws IOException {
        table.close();
    }
}
