/*
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
package io.trino.restful;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import static java.util.Objects.requireNonNull;

/**
 * @author duhanmin
 */
public class CatalogEntity
{

    private final String catalogName;

    private final String connectorName;

    private final Map<String, String> properties;

    //only when catalog update and catalog name is changed, the var has value
    private final String origCatalogName;

    @JsonCreator
    public CatalogEntity(
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("connectorName") String connectorName,
            @JsonProperty("properties") Map<String, String> properties,
            @JsonProperty("origCatalogName")  String origCatalogName
    )
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.properties = requireNonNull(properties, "properties is null");
        this.origCatalogName = origCatalogName;
    }


    @JsonProperty
    public String getCatalogName() {
        return catalogName;
    }

    @JsonProperty
    public String getConnectorName() {
        return connectorName;
    }

    @JsonProperty
    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonProperty
    public String getOrigCatalogName() { return origCatalogName; }
}
