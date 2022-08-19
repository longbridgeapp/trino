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
package io.trino.server;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.bval.util.StringUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.MoreObjects.firstNonNull;

public class YarnHeartbeat
{
    private static final Map<String, String> env = System.getenv();
    private static final String YARN_URL_API = "yarn_url_api";
    private static int num;
    private boolean yarn;
    private boolean heartbeat = true;
    private String yarnUrlApi;

    public boolean isYarn()
    {
        return yarn;
    }

    public YarnHeartbeat()
    {
        if (env.containsKey(YARN_URL_API)) {
            String yarnUrlApi = env.get(YARN_URL_API);
            if (StringUtils.isNotBlank(yarnUrlApi)) {
                this.yarn = true;
                this.yarnUrlApi = yarnUrlApi;
            }
        }
    }

    public void init(String version)
    {
        Thread main = new Thread(() -> new Server().start(firstNonNull(version, "unknown")));
        main.setName("main_yarn");
        main.start();
        while (heartbeat && num <= 2) {
            try {
                Thread.sleep(1000);
                Response execute = new OkHttpClient.Builder().readTimeout(5, TimeUnit.SECONDS).build().newCall(new Request.Builder().url(yarnUrlApi).build()).execute();
            }
            catch (Exception e) {
                num++;
            }
        }
        main.interrupt();
    }
}
