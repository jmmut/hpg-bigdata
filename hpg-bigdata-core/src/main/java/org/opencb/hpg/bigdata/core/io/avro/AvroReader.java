/*
 * Copyright 2015 OpenCB
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

package org.opencb.hpg.bigdata.core.io.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.opencb.commons.io.DataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by jmmut on 2015-05-05.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class AvroReader<T> implements DataReader<T> {
    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    private DataFileStream<T> dataFileReader;
    private final InputStream stream;

    public AvroReader(InputStream inputStream) {
        stream = inputStream;
    }

    @Override
    public boolean open() {
        DatumReader<T> datumReader = new SpecificDatumReader<>();
        try {
            dataFileReader = new DataFileStream<>(stream, datumReader);
        } catch (IOException e) {
            logger.error(e.toString()); // FIXME DataReader should throw IOException 
            return false;
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            dataFileReader.close();
        } catch (IOException e) {
            logger.error(e.toString()); // FIXME DataReader should throw IOException 
            return false;
        }
        return true;
    }

    @Override
    public List<T> read(int n) {
        List<T> list = new ArrayList<>(n);
        int i = 0;
        while (i < n && dataFileReader.hasNext()) {
            list.add(dataFileReader.next());
            i++;
        }
        logger.info("another batch of " + list.size() + " read");
        return list;
    }
    
    @Override
    public List<T> read() {
        return read(1);
    }
}
