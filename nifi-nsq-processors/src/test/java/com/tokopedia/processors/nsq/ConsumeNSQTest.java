/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tokopedia.processors.nsq;

import com.tokopedia.nsq.NSQProducer;
import com.tokopedia.nsq.exceptions.NSQException;
import com.tokopedia.nsq.lookup.DefaultNSQLookup;
import com.tokopedia.nsq.lookup.NSQLookup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class ConsumeNSQTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ConsumeNSQ.class);
    }

    @Test
    public void validateSuccessfullConsumeAndTransferToSuccess() throws InterruptedException, NSQException, TimeoutException {

        NSQProducer producer = new NSQProducer();
        producer.addAddress(NSQ.HOST, 4150);
        producer.start();
        String msg = "test-one-message";
        producer.produce(NSQ.TOPIC, msg.getBytes());
        producer.shutdown();

        ConsumeNSQ consumeNSQ = new ConsumeNSQ();
        TestRunner runner = TestRunners.newTestRunner(consumeNSQ);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConsumeNSQ.HOST, NSQ.HOST);
        runner.setProperty(ConsumeNSQ.CHANNEL, NSQ.CHANNEL);
        runner.setProperty(ConsumeNSQ.TOPIC, NSQ.TOPIC);
        runner.setProperty(ConsumeNSQ.MAX_MESSAGES_PER_BATCH, String.valueOf(NSQ.MAX_MSGS_PERT_BATCH));

        runner.run();
        Thread.sleep(5000);
        final MockFlowFile successFF = runner.getFlowFilesForRelationship(ConsumeNSQ.REL_SUCCESS).get(0);

        assertNotNull(successFF);

        runner.shutdown();

    }


    @Test
    public void testConsumeMultipleMessagesProcessor() throws InterruptedException, NSQException, TimeoutException {

        NSQProducer producer = new NSQProducer();
        producer.addAddress(NSQ.HOST, 4150);
        producer.start();
        String msg = "test-one-message";
        producer.produce(NSQ.TOPIC, msg.getBytes());
        producer.produce(NSQ.TOPIC, msg.getBytes());
        producer.produce(NSQ.TOPIC, msg.getBytes());
        producer.produce(NSQ.TOPIC, msg.getBytes());
        producer.produce(NSQ.TOPIC, msg.getBytes());
        producer.shutdown();

        ConsumeNSQ consumeNSQ = new ConsumeNSQ();
        TestRunner runner = TestRunners.newTestRunner(consumeNSQ);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConsumeNSQ.HOST, NSQ.HOST);
        runner.setProperty(ConsumeNSQ.CHANNEL, NSQ.CHANNEL);
        runner.setProperty(ConsumeNSQ.TOPIC, NSQ.TOPIC);
        runner.setProperty(ConsumeNSQ.MAX_MESSAGES_PER_BATCH, String.valueOf(NSQ.MAX_MSGS_PERT_BATCH));

        runner.run();
        Thread.sleep(5000);
        int ffs = runner.getFlowFilesForRelationship(ConsumeNSQ.REL_SUCCESS).size();

        assertEquals(5, ffs);



        runner.shutdown();

    }



}
