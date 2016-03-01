package org.opencb.hpg.bigdata.core.connectors;


import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantVcfFactory;
import org.opencb.biodata.models.variant.exceptions.NotAVariantException;
import org.opencb.biodata.tools.variant.converter.Converter;
import org.opencb.datastore.core.ObjectMap;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jmmut on 2015-12-17.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VcfToVariantConnector implements Connector<String, List<Variant>> {

    private VariantSource variantSource;

    public VcfToVariantConnector(VariantSource variantSource) {
        this.variantSource = variantSource;
    }

    @Override
    public ObjectMap getCredentials() {
        return null;
    }

    @Override
    public Converter<String, List<Variant>> getConverter() throws Exception {
        return line -> {
            List<Variant> parsed = new ArrayList<>();
            if (!line.startsWith("#") && !line.trim().isEmpty()) {
                VariantVcfFactory factory = new VariantVcfFactory();
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
            }

            return parsed;
        };
    }
}
