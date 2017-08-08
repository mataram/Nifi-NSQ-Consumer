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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"NSQ", "get", "message", "channel", "topic", "consumer", "receive", "que"})
@CapabilityDescription("Consumes NSQ Message transforming its content to a FlowFile and transitioning it to 'success' relationship\"")
public class ConsumeNSQ extends AbstractSessionFactoryProcessor {

    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("nsq.topic")
            .displayName("Topic Name")
            .description("The name of the NSQ Topic to pull from")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor CHANNEL = new PropertyDescriptor.Builder()
            .name("nsq.channel")
            .displayName("Channel")
            .description("Channel name")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("nsq.host")
            .displayName("Host")
            .description("Host of NSQ server")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("nsq.http.port")
            .displayName("Port")
            .description("Port of NSQ server")
            .required(true)
            .defaultValue("4161")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor MAX_MESSAGES_PER_BATCH = new PropertyDescriptor.Builder()
            .name("nsq.max.messages.per.batch")
            .displayName("Max messages per batch")
            .description("Specifies the maximum number of messages NSQ should return in a single poll.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles received from NSQ.  Depending on demarcation strategy it is a flow file per message or a bundle of messages grouped by topic and partition.")
            .build();
    public static final Relationship REL_ERROR = new Relationship.Builder()
            .name("error")
            .description("If any error occurred on NSQ client")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference = new AtomicReference<>();

    private NSQWorker nsqWorker;
    private List<NSQWorker> workers = new ArrayList<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(TOPIC);
        descriptors.add(CHANNEL);
        descriptors.add(HOST);
        descriptors.add(PORT);
        descriptors.add(MAX_MESSAGES_PER_BATCH);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_ERROR);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSessionFactory processSessionFactory) throws ProcessException {
        sessionFactoryReference.compareAndSet(null, processSessionFactory);

        final NSQWorker worker = getWorker(processContext);

        if (isScheduled()) {
            worker.getMoreMessages();
            getLogger().debug("NSQ get more messages");
        }

        processContext.yield();
    }


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }


    @OnStopped
    public void close() {
        getLogger().info("Close NSQ workers");
        if (nsqWorker != null) {
            try {
            for (NSQWorker worker : workers) {
                worker.close();
            }
            nsqWorker.close();
            nsqWorker = null;
            workers.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private NSQWorker getWorker(final ProcessContext context) {
        NSQWorker worker = nsqWorker;
        if (worker != null) {
            if (!worker.isStarted) {
                worker.start();
            }
            return worker;
        }

        nsqWorker = createWorker(context);
        workers.add(nsqWorker);
        nsqWorker.start();

        return nsqWorker;
    }

    // Create Worker instance
    private NSQWorker createWorker(final ProcessContext context) {
        return new NSQWorker(context.getProperty(HOST).getValue(),
                context.getProperty(PORT).asInteger(),
                context.getProperty(TOPIC).getValue(),
                context.getProperty(CHANNEL).getValue(),
                context.getProperty(MAX_MESSAGES_PER_BATCH).asInteger(),
                sessionFactoryReference);

    }

    protected void setWorker(NSQWorker worker) {
        this.nsqWorker = worker;
    }


}
