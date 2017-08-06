package org.wing4j.rrd.net.protocol;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/5.
 */
public class RoundRobinCreateTableProtocolV1Test {

    @Test
    public void testConvert() throws Exception {
        RoundRobinCreateTableProtocolV1 format = new RoundRobinCreateTableProtocolV1();
        format.setTableName("table1");
        format.setInstance("default");
        format.setColumns(new String[]{"request", "response"});
        format.setDataSize(10);
        ByteBuffer buffer = format.convert();
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        int messageType = buffer.getInt();
        Assert.assertEquals(MessageType.REQUEST.getCode(), messageType);
        RoundRobinCreateTableProtocolV1 format2 = new RoundRobinCreateTableProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals("default", format2.getInstance());
        Assert.assertEquals("table1", format2.getTableName());
        Assert.assertEquals("request", format2.getColumns()[0]);
        Assert.assertEquals("response", format2.getColumns()[1]);
        Assert.assertEquals(10, format2.getDataSize());
    }

    @Test
    public void testConvert1() throws Exception {

    }
}