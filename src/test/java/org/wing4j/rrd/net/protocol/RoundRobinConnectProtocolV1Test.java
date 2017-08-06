package org.wing4j.rrd.net.protocol;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/6.
 */
public class RoundRobinConnectProtocolV1Test {

    @Test
    public void testConvert() throws Exception {
        RoundRobinConnectProtocolV1 format = new RoundRobinConnectProtocolV1();
        format.setUsername("admin");
        format.setPassword("password");
        ByteBuffer buffer = format.convert();
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        int messageType = buffer.getInt();
        Assert.assertEquals(MessageType.REQUEST.getCode(), messageType);
        RoundRobinConnectProtocolV1 format2 = new RoundRobinConnectProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals("admin", format2.getUsername());
        Assert.assertEquals("password", format2.getPassword());
    }

    @Test
    public void testConvert1() throws Exception {

    }
}