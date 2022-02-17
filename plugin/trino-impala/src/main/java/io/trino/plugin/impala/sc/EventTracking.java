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
package io.trino.plugin.impala.sc;

import cn.hutool.cache.CacheUtil;
import cn.hutool.cache.impl.TimedCache;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.lang.Tuple;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.ContentType;
import cn.hutool.http.Header;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.json.JSONUtil;
import io.trino.plugin.impala.sc.entity.FieldSchema;
import io.trino.plugin.impala.sc.entity.TableSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: duhanmin
 * Description:
 * Date: 2022/2/16 10:45
 */
public class EventTracking
{
    private static final int CONNECTION_TIMEOUT = 60 * 1000;
    private static final int CACHE_TIMEOUT = 60 * CONNECTION_TIMEOUT;
    private static final String TABLE_SCHEMA = "tableSchema";
    private static final String SC_URL = "{}/api/sql/{}?project={}&token={}";
    private String url;
    private String project;
    private String token;

    private static Map<Header, String> header = new java.util.HashMap<>();
    private static TimedCache<String, Tuple> tableInfo = CacheUtil.newTimedCache(3 * CACHE_TIMEOUT);

    static {
        header.put(Header.CONTENT_TYPE, ContentType.JSON.getValue());
        tableInfo.schedulePrune(CACHE_TIMEOUT);
    }

    public EventTracking(String url, String project, String token)
    {
        this.url = url;
        this.project = project;
        this.token = token;
        init();
    }

    private void init()
    {
        String urlTable = StrUtil.format(SC_URL, url, "tables", project, token);
        List<String> tables = JSONUtil.toList(httpRequest(urlTable), String.class);

        String urlField = StrUtil.format(SC_URL, url, "meta", project, token);
        final List<TableSchema> tableSchemas = JSONUtil.toList(httpRequest(urlField), TableSchema.class);
        Map<String, List<FieldSchema>> map = new HashMap<>();
        for (TableSchema tableSchema : tableSchemas) {
            map.put(tableSchema.getName(), tableSchema.getColumns());
        }

        tableInfo.put(TABLE_SCHEMA, new Tuple(tables, map));
    }

    public List<String> getTables()
    {
        return getTuple().get(0);
    }

    public Map<String, List<FieldSchema>> getTableInfo()
    {
        return getTuple().get(1);
    }

    private Tuple getTuple()
    {
        if (!this.tableInfo.containsKey(TABLE_SCHEMA)) {
            init();
        }
        return this.tableInfo.get(TABLE_SCHEMA, false);
    }

    /**
     *
     * @param url
     * @return
     */
    private String httpRequest(String url)
    {
        final HttpRequest httpRequest = HttpRequest.get(url);
        httpRequest.setConnectionTimeout(CONNECTION_TIMEOUT);
        httpRequest.setReadTimeout(CONNECTION_TIMEOUT);
        if (MapUtil.isNotEmpty(header)) {
            for (Map.Entry<Header, String> entry : header.entrySet()) {
                httpRequest.header(entry.getKey(), entry.getValue());
            }
        }

        HttpResponse httpResponse = httpRequest.execute();

        if (!httpResponse.isOk() || StrUtil.isBlank(httpResponse.body()) || !JSONUtil.isJson(httpResponse.body())) {
            final String format = StrUtil.format("接口url:{}, header:{},返回body:{}异常", url, header, httpResponse.body());
            IoUtil.close(httpResponse);
            throw new RuntimeException(format);
        }

        final String httpResponseBody = httpResponse.body();
        IoUtil.close(httpResponse);

        return httpResponseBody;
    }
}
