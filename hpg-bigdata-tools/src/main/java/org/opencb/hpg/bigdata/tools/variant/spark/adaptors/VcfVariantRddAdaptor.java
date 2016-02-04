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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantVcfFactory;
import org.opencb.biodata.models.variant.exceptions.NotAVariantException;

import java.util.List;


/**
 * Created by jmmut on 2015-12-17.
 *
 * TODO jmmut: think about adding a sample set.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VcfVariantRddAdaptor implements VariantRddAdaptor {

    private String filename;

    /**
     * We use the constructor to ask for the VariantRddAdaptor-implementation-specific
     * required parameters.
     * @param filename vcf
     */
    public VcfVariantRddAdaptor(String filename) {
        this.filename = filename;
    }

    /**
     * @param ctx to create the RDD
     * @return JavaRDD of variants
     */
    @Override
    public JavaRDD<Variant> getRdd(JavaSparkContext ctx) {

        JavaRDD<String> file = ctx.textFile(this.filename);

        return file
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
    }
}
