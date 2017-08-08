package com.tokopedia.nsq.callbacks;

import com.tokopedia.nsq.NSQMessage;

@FunctionalInterface
public interface NSQMessageCallback {

	public void message(NSQMessage message);
}
