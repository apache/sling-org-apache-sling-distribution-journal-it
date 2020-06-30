/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.distribution.journal.it;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Client {
    private static final String ADMIN_USER = "admin";
    private static final String ADMIN_PASSWORD = "admin";
    private static final int TIMEOUT_SECONDS = 60; 
    
    protected static Logger LOG = LoggerFactory.getLogger(Client.class);
    
    public static void waitNumQueues(int num) {
        await("Waiting for " + num + " queues to be present")
            .pollInterval(1,  TimeUnit.SECONDS)
            .timeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .until(Client::numQueues, equalTo(num));
    }
    
    public static void waitSumQueueSizes(int num) {
        await("Wait for all queues to have size sum " + num)
        .pollInterval(1, TimeUnit.SECONDS)
        .timeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .ignoreExceptions()
        .until(Client::sumQueueSizes, equalTo(num));
    }
    
    public static int numQueues() {
        return getQueues(DistributionTestBase.PUB1_AGENT).size();
    }
    
    public static int sumQueueSizes() {
        Map<String, Integer> queues = Client.getQueues(DistributionTestBase.PUB1_AGENT);
        
        return queues.values().stream().mapToInt(Integer::valueOf).sum();
    }

    public static Map<String, Integer> getQueues(String agentName) {
        String uri = String.format("http://localhost:%s/libs/sling/distribution/services/agents/%s/queues.2.json", 
                8181, 
                agentName);
        JsonObject root = getJson(uri).getAsJsonObject();
        Map<String, Integer> queues = new HashMap<>();
        root.entrySet().stream()
            .filter(entry -> !entry.getKey().equals("items"))
            .filter(entry -> !entry.getKey().equals("sling:resourceType"))
            .forEach(entry -> {
                queues.put(entry.getKey(), getItemsCount(entry));
            });
        LOG.info("Queue sizes {}", queues);
        return queues;
    }
    
    private static int getItemsCount(Entry<String, JsonElement> entry) {
        return entry.getValue().getAsJsonObject().get("itemsCount").getAsInt();
    }
    
    public static List<String> getSubNodes(String uri) {
        LOG.info("Trying to get queue from {}", uri);
        JsonElement root = getJson(uri);
        List<String> result = new ArrayList<>();
        JsonArray items = root.getAsJsonObject().get("items").getAsJsonArray();
        items.forEach(item -> result.add(item.getAsString()));
        return result;
    }
    
    private static JsonElement getJson(String uri) {
        try (CloseableHttpClient client =  createHttpClient()) {
            HttpGet httpGet = new HttpGet(uri);
            CloseableHttpResponse response = client.execute(httpGet);
            InputStreamReader reader = new InputStreamReader(response.getEntity().getContent(), Charset.forName("utf-8"));
            return new JsonParser().parse(reader);
        } catch (Exception e) {
            throw new RuntimeException("Cannot get json for uri " + uri, e);
        }
    }

    protected static CloseableHttpClient createHttpClient() {
        return HttpClientBuilder.create()
                .setDefaultCredentialsProvider(credentialsProvider()).build();
    }

    private static CredentialsProvider credentialsProvider() {
        CredentialsProvider provider = new BasicCredentialsProvider();
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(ADMIN_USER, ADMIN_PASSWORD);
        provider.setCredentials(AuthScope.ANY, credentials);
        return provider;
    }
}
