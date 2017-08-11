package org.wing4j.rrd.net.protocol;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/6.
 */
public class RoundRobinDisConnectProtocolV1Test {

    @Test
    public void testConvert() throws Exception {
        RoundRobinDisConnectProtocolV1 format = new RoundRobinDisConnectProtocolV1();
        format.setSessionId("11111");
        ByteBuffer buffer = format.convert();
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        int messageType = buffer.getInt();
        Assert.assertEquals(MessageType.REQUEST.getCode(), messageType);
        RoundRobinDisConnectProtocolV1 format2 = new RoundRobinDisConnectProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals("11111", format2.getSessionId());
    }

    @Test
    public void testConvert1() throws Exception {

    }
}