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

import java.io.IOException;

import org.apache.sling.distribution.journal.it.Client;
import org.apache.sling.distribution.journal.it.DistributionTestBase;
import org.apache.sling.distribution.journal.it.ext.AfterOsgi;
import org.apache.sling.distribution.journal.it.ext.BeforeOsgi;
import org.apache.sling.distribution.journal.it.ext.ExtPaxExam;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.TestContainer;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;


@RunWith(ExtPaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class StagedDistributionFailureTest extends DistributionTestBase {

    private static final String SUB1_AGENT = PUB1_AGENT + "Subscriber";
    private static final String SUB2_AGENT = PUB1_AGENT + "Subscriber";

    private static volatile TestContainer publish;
    private static volatile TestContainer golden_publish;

    private static final String TEST_PATH = "/content/mytest";


    @BeforeOsgi
    public static void beforeOsgi() throws Exception {
        beforeOsgiBase();
        publish = startPublishInstance(8182,  SUB1_AGENT, false,  true);
        new Thread(StagedDistributionFailureTest::delayedStartGoldenSubscriber).start();
    }
    
    /**
     * Wait for  at least one item in publish queue before starting golden publish
     */
    private static void delayedStartGoldenSubscriber() {
        try {
            Client.waitSumQueueSizes(1);
            LOG.info("Starting golden publish");
            golden_publish = startPublishInstance(8183, SUB2_AGENT, true, false);
        } catch (Exception e) {
            LOG.error("Start of golden subscriber failed with: " + e.getMessage(), e);
        }
    }

    @AfterOsgi
    public static void afterOsgi() throws IOException {
        if (publish != null) {
            publish.stop();
        }
        if (golden_publish != null) {
            golden_publish.stop();
        }

        afterOsgiBase();
    }

    @Before
    public void before() {
        createPath(TEST_PATH);
        Client.waitNumQueues(1);
    }

    /**
     * Start just a regular subscriber and do a distribution. 
     * We expect that this subscriber does not process the package as it wait on the golden subscriber.
     * After the message is present in the queue we start the golden subscriber.
     * Now first golden then regular subscriber should be able to process the distribution package.
     */
    @Test
    public void testDistribute() {
        distribute(TEST_PATH);

        Client.waitNumQueues(2);
        Client.waitSumQueueSizes(0);

        waitPath(8182, TEST_PATH);
        waitPath(8183, TEST_PATH);
    }


}
