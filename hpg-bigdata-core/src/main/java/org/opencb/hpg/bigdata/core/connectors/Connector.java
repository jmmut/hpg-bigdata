package org.opencb.hpg.bigdata.core.connectors;

import org.opencb.biodata.tools.variant.converter.Converter;
import org.opencb.datastore.core.ObjectMap;

/**
 * Created by jmmut on 2015-12-17.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public interface Connector<InputType, OutputType> {
    /**
     * object map with all the data required to connect to the data source, such as user, password, location,
     * table names, filters, etc.
     * @return ObjectMap of credential values
     */
    ObjectMap getCredentials();

    /**
     * Polymorphism to convert storage format to model beans.
     * @return an instance of a specific converter
     * @throws Exception in case anything goes wrong. Perhaps it is interesting for implementers to throw
     * Exceptions of a more specific type.
     */
    Converter<InputType, OutputType> getConverter() throws Exception;

}
