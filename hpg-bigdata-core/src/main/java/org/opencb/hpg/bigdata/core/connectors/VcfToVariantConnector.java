package org.opencb.hpg.bigdata.core.connectors;

import com.fasterxml.jackson.databind.util.Converter;
import org.opencb.datastore.core.ObjectMap;

/**
 * Created by jmmut on 2015-12-17.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class VcfToVariantConnector implements Connector {


    @Override
    public ObjectMap getCredentials() {
        return null;
    }

    @Override
    public Converter getConverter() {
        return null;
    }
}
