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
import static org.junit.Assert.assertThat;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.newConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageInfo;
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
public class AuthorRestartTest extends DistributionTestSupport {
    private static final String PUB1_AGENT = "agent1";
    private static final String SUB1_SLING_ID = UUID.randomUUID().toString();
    private static final String SUB1_AGENT = "sub1agent";
    private static final String QUEUE_NAME = SUB1_SLING_ID + "-" + SUB1_AGENT;
    private static final int NUM_MESSAGES = 2000;

    Logger log = LoggerFactory.getLogger(this.getClass());

    @Inject
    @Filter(value = "(name=agent1)", timeout = 40000L)
    private DistributionAgent agent;
    
    @Inject
    @Filter
    private ResourceResolverFactory resourceResolverFactory;
    
    @Inject
    private MessagingProvider clientProvider;
    
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
    public void testRestartWithExisingMessages() throws Exception {
        for (int c = 0; c < NUM_MESSAGES; c++) {
            if (c % 100 == 0) {
                log.info("Sending message {}", c);
            }
            PackageMessage packageMessage = createPackageMessage(c);
            clientProvider.createSender(TOPIC_PACKAGE).send(packageMessage);
        }
        try (Closeable packagePoller = createPoller()) {
            messageSem.tryAcquire(NUM_MESSAGES, 100, TimeUnit.SECONDS);
        }
        await().until(() -> toSet(agent.getQueueNames()), equalTo(Collections.emptySet()));
        DiscoveryMessage disc = createDiscoveryMessage(-1);
        clientProvider.createSender(TOPIC_DISCOVERY).send(disc);
        await().until(() -> toSet(agent.getQueueNames()), equalTo(Collections.singleton(QUEUE_NAME)));
        
        log.info("Checking Items in queue");
        for (int c=0; c<20; c++) {
            int itemsCount = agent.getQueue(QUEUE_NAME).getStatus().getItemsCount();
            log.info("Items in queue: {}", itemsCount);
            assertThat(itemsCount, equalTo(NUM_MESSAGES));
            Thread.sleep(100);
        }
    }

    private Closeable createPoller() {
        HandlerAdapter<PackageMessage> adapter = create(PackageMessage.class, this::handle);
        return clientProvider.createPoller(TOPIC_PACKAGE, Reset.earliest, adapter);
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
                .subscriberStates(Collections.singletonList(subState))
                .build();
    }

    private <T> Set<T> toSet(final Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false)
                            .collect(Collectors.toSet());
    }
    
    private PackageMessage createPackageMessage(int num) throws IOException {
        return PackageMessage.builder()
                .pkgId("myid" + num)
                .pubSlingId("pub1sling")
                .pubAgentName(PUB1_AGENT)
                .pkgType("journal")
                .reqType(PackageMessage.ReqType.ADD)
                .paths(Arrays.asList("/test"))
                .pkgBinary(new byte[100])
                .build();
    }
    
    private void handle(MessageInfo info, PackageMessage message) {
        if (message.getReqType() == ReqType.ADD) {
            recordedPackage.set(message);
            messageSem.release();
        }
    }
}
