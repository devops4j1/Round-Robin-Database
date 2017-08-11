package org.wing4j.rrd.net.protocol;

import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/4.
 * 协议接口
 */
public interface RoundRobinProtocol {
    ByteBuffer convert();
    void convert(ByteBuffer buffer);
}
