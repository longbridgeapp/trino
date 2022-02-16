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
    private static Map<Header, String> header = new java.util.HashMap<>();
    private static final int CONNECTION_TIMEOUT = 60 * 1000;
    private static final int CACHE_TIMEOUT = 60 * CONNECTION_TIMEOUT;
    public static final String TABLES = "tables";
    public static final String TABLE_SCHEMA = "tableSchema";

    private static TimedCache<String, List<String>> tables = CacheUtil.newTimedCache(3 * CACHE_TIMEOUT);
    private static TimedCache<String, Map<String, List<FieldSchema>>> tableInfo = CacheUtil.newTimedCache(3 * CACHE_TIMEOUT);

    private String url;
    private String scUser;
    private String scPwd;

    static {
        header.put(Header.CONTENT_TYPE, ContentType.JSON.getValue());
        tables.schedulePrune(CACHE_TIMEOUT);
        tableInfo.schedulePrune(CACHE_TIMEOUT);
    }

    public EventTracking(String url, String scUser, String scPwd)
    {
        this.url = url;
        this.scUser = scUser;
        this.scPwd = scPwd;
        init();
    }

    private void init()
    {
        String urlTable = StrUtil.format("{}/api/sql/tables?project={}&token={}", url, scUser, scPwd);
        String urlField = StrUtil.format("{}/api/sql/meta?project={}&token={}", url, scUser, scPwd);

        List<String> tables = JSONUtil.toList(httpRequest(urlTable), String.class);
        this.tables.put(TABLES,tables);
        final List<TableSchema> tableSchemas = JSONUtil.toList(httpRequest(urlField), TableSchema.class);
        Map<String, List<FieldSchema>> map = new HashMap<>();
        for (TableSchema tableSchema : tableSchemas) {
            map.put(tableSchema.getName(),tableSchema.getColumns());
        }
        this.tableInfo.put(TABLE_SCHEMA,map);
    }

    public List<String> getTables()
    {
        if (!this.tables.containsKey(TABLES))
            init();
        return this.tables.get(TABLES);
    }

    public Map<String, List<FieldSchema>> getTableInfo()
    {
        if (!this.tableInfo.containsKey(TABLE_SCHEMA))
            init();
        return this.tableInfo.get(TABLE_SCHEMA);
    }

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
