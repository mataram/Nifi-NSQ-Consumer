package com.tokopedia.processors.nsq;

public class NSQ {
    protected static final String HOST = "localhost";
    protected static final int PORT = 4161;
    protected static final String TOPIC = "test_nifi";
    protected static final String CHANNEL = "ch_nifi";
    protected static final int MAX_MSGS_PERT_BATCH = 5;
}
