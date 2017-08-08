package com.tokopedia.processors.nsq;

import com.tokopedia.nsq.NSQConfig;
import com.tokopedia.nsq.NSQConsumer;
import com.tokopedia.nsq.callbacks.NSQErrorCallback;
import com.tokopedia.nsq.callbacks.NSQMessageCallback;
import com.tokopedia.nsq.lookup.DefaultNSQLookup;
import com.tokopedia.nsq.lookup.NSQLookup;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class NSQWorker implements Closeable{
    private String host;
    private int port;
    private String topic;
    private String channel;

    private int maxMessagePerBatch;

    private NSQConsumer consumer;
    private NSQMessageCallback messageCallback;
    private NSQErrorCallback errorCallback;

    private AtomicReference<ProcessSessionFactory> sessionFactoryHolder;
    public boolean isStarted = false;

    public NSQWorker(String host, int port, String topic, String channel, int maxMessagePerBatch, AtomicReference<ProcessSessionFactory> processSessionFactory) {
        this.host = host;
        this.port = port;
        this.topic = topic;
        this.channel = channel;
        this.maxMessagePerBatch = maxMessagePerBatch;
        this.sessionFactoryHolder = processSessionFactory;
    }

    public void start() {
        messageCallback = getMessageCallback();
        errorCallback = getErrorCallback();
        if (consumer == null) {
            isStarted = false;
            NSQLookup lookup = new DefaultNSQLookup();
            lookup.addLookupAddress(host, port);
            consumer = new NSQConsumer(lookup,
                    topic,
                    channel,
                    messageCallback,
                    new NSQConfig(),
                    errorCallback);

        }

        consumer.setMessagesPerBatch(maxMessagePerBatch);
        consumer.start();
        isStarted = true;
    }

    private NSQMessageCallback getMessageCallback() {
        if (messageCallback == null) {
            messageCallback = nsqMessage -> {

                ProcessSessionFactory sessionFactory;
                do {
                    sessionFactory = sessionFactoryHolder.get();
                    if (sessionFactory == null) {
                        try {
                            Thread.sleep(10);
                        } catch (final InterruptedException e) {
                        }
                    }
                } while (sessionFactory == null);

                final ProcessSession session = sessionFactory.createSession();

                FlowFile flowFile = session.create();
                flowFile = session.write(flowFile, outputStream -> outputStream.write(nsqMessage.getMessage()));

                final Map<String, String> attributes = new HashMap<>();

                attributes.put("message_id", new String(nsqMessage.getId()));
                attributes.put("topic", topic);
                attributes.put("channel", channel);
                attributes.put("timestamp", String.valueOf(nsqMessage.getTimestamp().getTime()));

                final FlowFile newFlowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(newFlowFile, ConsumeNSQ.REL_SUCCESS);
                session.commit();

                nsqMessage.finished();
            };

        }
        return messageCallback;
    }

    private NSQErrorCallback getErrorCallback() {
        if (errorCallback == null) {
            errorCallback = e -> {
                ProcessSessionFactory sessionFactory;
                do {
                    sessionFactory = sessionFactoryHolder.get();
                    if (sessionFactory == null) {
                        try {
                            Thread.sleep(10);
                        } catch (final InterruptedException ex) {
                        }
                    }
                } while (sessionFactory == null);

                final ProcessSession session = sessionFactory.createSession();

                FlowFile flowFile = session.create();
                flowFile = session.write(flowFile, outputStream -> outputStream.write(e.getMessage().getBytes()));

                flowFile = session.putAttribute(flowFile, "local_message", e.getLocalizedMessage());
                session.transfer(flowFile, ConsumeNSQ.REL_ERROR);
            };
        }

        return errorCallback;
    }


    @Override
    public void close() throws IOException {
        isStarted = false;
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    public void getMoreMessages() {
        consumer.getMoreMessage();
    }

}
