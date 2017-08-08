package com.tokopedia.nsq.callbacks;

import com.tokopedia.nsq.exceptions.NSQException;

@FunctionalInterface
public interface NSQErrorCallback {

    void error(NSQException x);
}
