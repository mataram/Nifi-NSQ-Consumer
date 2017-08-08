package com.tokopedia.nsq.lookup;

import com.tokopedia.nsq.ServerAddress;
import com.tokopedia.nsq.ServerAddress;

import java.util.Set;

public interface NSQLookup {
    Set<ServerAddress> lookup(String topic);

    void addLookupAddress(String addr, int port);
}
