package org.opencb.hpg.bigdata.core.connectors;

import com.fasterxml.jackson.databind.util.Converter;
import org.opencb.datastore.core.ObjectMap;

/**
 * Created by jmmut on 2015-12-17.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public interface Connector {
    /**
     * object map with all the data required to connect to the data source, such as user, password, location,
     * table names, filters, etc.
     * @return ObjectMap of credential values
     */
    ObjectMap getCredentials();

    /**
     * Polymorphism to convert storage format to model beans.
     * @return an instance of a specific converter
     */
    Converter getConverter();

}
