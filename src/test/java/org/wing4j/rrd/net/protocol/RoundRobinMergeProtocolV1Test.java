package org.wing4j.rrd.net.protocol;

import org.junit.Assert;
import org.junit.Test;
import org.wing4j.rrd.*;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.utils.HexUtils;

import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/4.
 */
public class RoundRobinMergeProtocolV1Test {
    @Test
    public void testRead() throws Exception {
        long[][] data = new long[2][2];
        RoundRobinMergeProtocolV1 format = new RoundRobinMergeProtocolV1();
        format.setMessageType(MessageType.REQUEST);
        format.setData(data);
        format.setColumns(new String[]{"request", "response"});
        format.setPos(2);
        format.setMergeType(MergeType.ADD);
        format.setTableName("mo");
        ByteBuffer buffer = format.convert();
        System.out.println(HexUtils.toDisplayString(buffer.array()));
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        int messageType = buffer.getInt();
        Assert.assertEquals(MessageType.REQUEST.getCode(), messageType);
        RoundRobinMergeProtocolV1 format2 = new RoundRobinMergeProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals("mo", format2.getTableName());
        Assert.assertEquals("request", format2.getColumns()[0]);
    }

    @Test
    public void testWrite() throws Exception {
        long[][] data = new long[2][2];
        RoundRobinMergeProtocolV1 format = new RoundRobinMergeProtocolV1();
        format.setMessageType(MessageType.RESPONSE);
        format.setData(data);
        format.setColumns(new String[]{"request", "response"});
        format.setPos(2);
        format.setMergeType(MergeType.ADD);
        format.setTableName("mo");
        ByteBuffer buffer = format.convert();
        System.out.println(HexUtils.toDisplayString(buffer.array()));
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        int messageType = buffer.getInt();
        Assert.assertEquals(MessageType.RESPONSE.getCode(), messageType);
        RoundRobinMergeProtocolV1 format2 = new RoundRobinMergeProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals("mo", format2.getTableName());
        Assert.assertEquals("request", format2.getColumns()[0]);
        Assert.assertEquals("response", format2.getColumns()[1]);
        Assert.assertEquals(2, format2.getPos());
    }
}