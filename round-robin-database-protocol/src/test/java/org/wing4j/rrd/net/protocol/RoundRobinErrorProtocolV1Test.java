package org.wing4j.rrd.net.protocol;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Created by 面试1 on 2017/8/7.
 */
public class RoundRobinErrorProtocolV1Test {

    @Test
    public void testConvert() throws Exception {
        RoundRobinErrorProtocolV1 format = new RoundRobinErrorProtocolV1();
        format.setCode(RspCode.FAIL.getCode());
        format.setDesc(RspCode.FAIL.getDesc());
        ByteBuffer buffer = format.convert();
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        int messageType = buffer.getInt();
        Assert.assertEquals(MessageType.REQUEST.getCode(), messageType);
        RoundRobinErrorProtocolV1 format2 = new RoundRobinErrorProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals(RspCode.FAIL.getCode(), format2.getCode());
        Assert.assertEquals(RspCode.FAIL.getDesc(), format2.getDesc());
    }

    @Test
    public void testConvert1() throws Exception {

    }
}