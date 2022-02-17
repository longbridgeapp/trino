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
package io.trino.plugin.impala;

import com.cloudera.impala.jdbc.Driver;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.trino.plugin.jdbc.BaseJdbcConfig;

import javax.validation.constraints.AssertTrue;

public class ImpalaJdbcConfig
        extends BaseJdbcConfig

{
    private String url;
    private String project;
    private String token;

    @AssertTrue(message = "Invalid JDBC URL for MySQL connector")
    public boolean isUrlValid()
    {
        try {
            Driver driver = new Driver();
            return driver.acceptsURL(getConnectionUrl());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getUrl()
    {
        return url;
    }

    @Config("sc.url")
    @ConfigDescription("sc.url")
    public ImpalaJdbcConfig setUrl(String url)
    {
        this.url = url;
        return this;
    }

    public String getProject()
    {
        return project;
    }

    @Config("sc.project")
    @ConfigDescription("sc.project")
    public ImpalaJdbcConfig setProject(String project)
    {
        this.project = project;
        return this;
    }

    public String getToken()
    {
        return token;
    }

    @Config("sc.token")
    @ConfigDescription("sc.token")
    public ImpalaJdbcConfig setToken(String token)
    {
        this.token = token;
        return this;
    }
}
