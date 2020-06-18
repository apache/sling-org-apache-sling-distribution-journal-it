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
package org.apache.sling.distribution.journal.it.tests;

import static org.apache.sling.distribution.journal.HandlerAdapter.create;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.newConfiguration;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.DistributionResponse;
import org.apache.sling.distribution.Distributor;
import org.apache.sling.distribution.SimpleDistributionRequest;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.it.DistributionTestSupport;
import org.apache.sling.distribution.journal.it.kafka.PaxExamWithKafka;
import org.apache.sling.distribution.journal.messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.messages.SubscriberConfig;
import org.apache.sling.distribution.journal.messages.SubscriberState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.ops4j.pax.exam.util.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts an author instance, triggers a content distribution and checks that the package arrives
 * on the journal.
 */
@RunWith(PaxExamWithKafka.class)
@ExamReactorStrategy(PerClass.class)
public class AuthorDistributeTest extends DistributionTestSupport {
    private static final String PUB1_AGENT = "agent1";
    private static final String SUB1_SLING_ID = UUID.randomUUID().toString();
    private static final String SUB1_AGENT = "sub1agent";
    private static final String QUEUE_NAME = SUB1_SLING_ID + "-" + SUB1_AGENT;

    Logger log = LoggerFactory.getLogger(this.getClass());

    @Inject
    @Filter(value = "(name=agent1)", timeout = 40000L)
    DistributionAgent agent;
    
    @Inject
    @Filter
    Distributor distributor;

    @Inject
    @Filter
    ResourceResolverFactory resourceResolverFactory;
    
    @Inject
    MessagingProvider clientProvider;
    
    private AtomicReference<PackageMessage> recordedPackage = new AtomicReference<PackageMessage>();

    private Semaphore messageSem = new Semaphore(0);
    
    @Configuration
    public Option[] configuration() {
        return new Option[] { //
                //debug(),
                newConfiguration("org.apache.sling.jcr.base.internal.LoginAdminWhitelist")
                        .put("whitelist.bypass", "true").asOption(),
                baseConfiguration(), //
                defaultOsgiConfigs(), //
                authorOsgiConfigs() //
        };
    }

    @Test
    public void testDistribute() throws Exception {
        await().until(this::distribute);
        
        try (Closeable packagePoller = createPoller()) {
            assertPackageReceived();
        }
        
        simulateDiscoveryMessage(-1);
        await().until(() -> toSet(agent.getQueueNames()), equalTo(Collections.singleton(QUEUE_NAME)));
        assertThat(agent.getQueue(QUEUE_NAME).getStatus().getItemsCount(), equalTo(1));
    }

    @SuppressWarnings("deprecation")
    private boolean distribute() throws LoginException {
        Map<String, Object> authinfo = new HashMap<String, Object>();
        try (ResourceResolver resolver = resourceResolverFactory.getAdministrativeResourceResolver(authinfo)) {
            DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, "/");
            DistributionResponse response = distributor.distribute(PUB1_AGENT, resolver, request);
            log.info(response.getMessage());
            return response.isSuccessful();
        }
    }

    private Closeable createPoller() {
        HandlerAdapter<PackageMessage> adapter = create(PackageMessage.class, this::handle);
        return clientProvider.createPoller(TOPIC_PACKAGE, Reset.earliest, adapter);
    }
    
    private void assertPackageReceived() throws InterruptedException {
        assertTrue(messageSem.tryAcquire(10, TimeUnit.SECONDS));
        PackageMessage pkg = recordedPackage.get();
        assertEquals(PackageMessage.ReqType.ADD, pkg.getReqType());
        String path = pkg.getPaths().iterator().next();
        assertEquals("/", path);
    }

    private void simulateDiscoveryMessage(long offset) {
        MessageSender<DiscoveryMessage> discSender = clientProvider.createSender(TOPIC_DISCOVERY);
        DiscoveryMessage disc = createDiscoveryMessage(offset);
        discSender.accept(disc);
    }

    private DiscoveryMessage createDiscoveryMessage(long offset) {
        SubscriberState subState = SubscriberState.builder()
                .offset(offset)
                .pubAgentName(PUB1_AGENT)
                .build();
        return DiscoveryMessage.builder()
                .subSlingId(SUB1_SLING_ID)
                .subAgentName(SUB1_AGENT)
                .subscriberConfiguration(SubscriberConfig
                        .builder()
                        .editable(false)
                        .maxRetries(-1)
                        .build())
                .subscriberStates(Arrays.asList(subState))
                .build();
    }

    private static <T> Set<T> toSet(final Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false)
                            .collect(Collectors.toSet());
    }
    
    void handle(MessageInfo info, PackageMessage message) {
        if (message.getReqType() == ReqType.ADD) {
            recordedPackage.set(message);
            messageSem.release();
        }
    }
}
