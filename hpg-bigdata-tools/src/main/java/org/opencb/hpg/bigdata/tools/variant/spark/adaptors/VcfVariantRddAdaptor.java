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

package org.opencb.hpg.bigdata.tools.variant.spark.adaptors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantVcfFactory;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.exceptions.NotAVariantException;

import java.io.IOException;
import java.util.List;


/**
 * Created by jmmut on 2015-12-17.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VcfVariantRddAdaptor implements VariantRddAdaptor {

    private String filename;

    /**
     * @param filename vcf
     */
    public VcfVariantRddAdaptor(String filename) {
        this.filename = filename;
    }

    /**
     * TODO jmmut: think about adding a sample set.
     * @return JavaRDD of variants
     * @throws IOException using outputstreams
     */
    @Override
    public JavaRDD<Variant> getRdd() throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("JavaRowCount").setMaster("local[3]");    // 3 threads
        sparkConf.registerKryoClasses(new Class[]{VariantAvro.class});
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> file = ctx.textFile(this.filename);
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

        return variants;
    }
}
