package org.wing4j.rrd.net.protocol;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;


/**
 * Created by wing4j on 2017/8/4.
 */
public class RoundRobinExpandProtocolV1Test {

    @Test
    public void testConvert() throws Exception {
        RoundRobinExpandProtocolV1 format = new RoundRobinExpandProtocolV1();
        format.setTableName("table1");
        format.setColumns(new String[]{"request", "response"});
        ByteBuffer buffer = format.convert();
//        System.out.println(HexUtils.toDisplayString(buffer.array()));
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        int messageType = buffer.getInt();
        Assert.assertEquals(MessageType.REQUEST.getCode(), messageType);
        RoundRobinExpandProtocolV1 format2 = new RoundRobinExpandProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals("table1", format2.getTableName());
        Assert.assertEquals("request", format2.getColumns()[0]);
        Assert.assertEquals("response", format2.getColumns()[1]);
    }

    @Test
    public void testConvert1() throws Exception {
        RoundRobinExpandProtocolV1 format = new RoundRobinExpandProtocolV1();
        format.setTableName("table1");
        format.setColumns(new String[]{"request", "response"});
        ByteBuffer buffer = format.convert();
//        System.out.println(HexUtils.toDisplayString(buffer.array()));
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        int messageType = buffer.getInt();
        Assert.assertEquals(MessageType.REQUEST.getCode(), messageType);
        RoundRobinExpandProtocolV1 format2 = new RoundRobinExpandProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals("table1", format2.getTableName());
        Assert.assertEquals("request", format2.getColumns()[0]);
        Assert.assertEquals("response", format2.getColumns()[1]);
    }
}