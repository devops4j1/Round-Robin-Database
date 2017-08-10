package org.wing4j.rrd.net.listener;

import java.io.IOException;
import java.nio.channels.Selector;

/**
 * Created by wing4j on 2017/8/10.
 */
public interface RoundRobinReadWriteHandler {
    void register(Selector selector) throws IOException;
    void asyncRead() throws IOException;
    void doNextWriteCheck();
}
