package org.wing4j.rrd.net.protocol;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Created by wing4j on 2017/8/6.
 */
public class RoundRobinDropTableProtocolV1Test {

    @Test
    public void testConvert() throws Exception {
        RoundRobinDropTableProtocolV1 format = new RoundRobinDropTableProtocolV1();
        format.setTableNames(new String[]{"table1"});
        format.setInstance("default");
        ByteBuffer buffer = format.convert();
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        int messageType = buffer.getInt();
        Assert.assertEquals(MessageType.REQUEST.getCode(), messageType);
        RoundRobinDropTableProtocolV1 format2 = new RoundRobinDropTableProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals("default", format2.getInstance());
        Assert.assertEquals("table1", format2.getTableNames()[0]);
    }

    @Test
    public void testConvert1() throws Exception {

    }
}