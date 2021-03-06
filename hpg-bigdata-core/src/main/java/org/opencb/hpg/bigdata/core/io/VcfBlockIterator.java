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

/**
 *
 */
package org.opencb.hpg.bigdata.core.io;

import htsjdk.tribble.readers.LineIterator;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.opencb.commons.io.DataReader;
import org.opencb.hpg.bigdata.core.converters.FullVcfCodec;

import java.io.*;
import java.nio.CharBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * @author mh719
 *
 */
public class VcfBlockIterator implements AutoCloseable, Iterator<List<CharBuffer>>, Iterable<List<CharBuffer>> {
    private static final long DEFAULT_64KB_BLOCK = 64L * 1024L;

    private final File file;
    private final InputStream in;
    private final LineIterator iter;

    private final AtomicLong charBlockSize = new AtomicLong(DEFAULT_64KB_BLOCK);

    private final VCFHeader header;

    private final VCFHeaderVersion version;

    public VcfBlockIterator(File vcfFile) throws IOException {
        this(vcfFile, new FullVcfCodec());
    }

    public VcfBlockIterator(File vcfFile, FullVcfCodec codec) throws IOException {
        this.file = vcfFile;
        this.in = buildInputStream(this.file);
        this.iter = codec.makeSourceFromStream(this.in);
        this.header = (VCFHeader) codec.readActualHeader(this.iter);
        this.version = codec.getVCFHeaderVersion();
    }

    public VcfBlockIterator(InputStream in, FullVcfCodec codec) throws IOException {
        this.file = null;
        this.in = in;
        this.iter = codec.makeSourceFromStream(this.in);
        this.header = (VCFHeader) codec.readActualHeader(this.iter);
        this.version = codec.getVCFHeaderVersion();
    }

    public VCFHeader getHeader() {
        return this.header;
    }

    public VCFHeaderVersion getVersion() {
        return version;
    }

    @Override
    public List<CharBuffer> next() {
        return next(this.charBlockSize.get());
    }

    public List<CharBuffer> next(long blockSize) {
        long cnt = 0L;
        List<CharBuffer> next = new LinkedList<>(); // linked list faster at creation time
        while (iter.hasNext() && cnt < blockSize) {
            String line = iter.next();
            CharBuffer buff = CharBuffer.wrap(line.toCharArray());  //FIXME! Avoid char array copy
            next.add(buff);
            cnt += buff.length();
        }
        return next;
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    protected InputStream  buildInputStream(File inFile) throws IOException {
        InputStream  inputStream = new FileInputStream(inFile);
        String name = inFile.getName();
        String ext = FilenameUtils.getExtension(name);
        switch (ext) {
            case "gz":
            case "gzip":
                inputStream = new GZIPInputStream(inputStream);
                break;
            case "vcf":
            case "txt":
            case "tsv":
                //nothing to do
                break;
            default:
                throw new NotImplementedException(String.format("Compression extension %s not yet supported!!!", ext));
        }
        return new BufferedInputStream(inputStream);
    }

    @Override
    public void close() throws IOException {
        this.in.close();
    }

    @Override
    public void remove() {
        throw new NotImplementedException("Remove not implemented");
    }

    @Override
    public Iterator<List<CharBuffer>> iterator() {
        return this;
    }

    public DataReader<CharBuffer> toCharBufferDataReader() {
        return new DataReader<CharBuffer>() {
            @Override
            public List<CharBuffer> read(int size) {
                return (hasNext() ? next(size) : Collections.<CharBuffer>emptyList());
            }

            @Override
            public boolean close() {
                try {
                    VcfBlockIterator.this.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }
        };
    }

    public DataReader<CharSequence> toLineDataReader() {
        return new DataReader<CharSequence>() {
            @Override
            public List<CharSequence> read(int size) {
                List<CharSequence> batch = new ArrayList<>(size);
                for (int i = 0; i < size && iter.hasNext(); i++) {
                    batch.add(iter.next());
                }
                return batch;
            }

            @Override
            public boolean close() {
                try {
                    VcfBlockIterator.this.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }
        };
    }
}
