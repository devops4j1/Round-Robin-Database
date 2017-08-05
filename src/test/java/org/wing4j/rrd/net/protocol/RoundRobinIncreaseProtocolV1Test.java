package org.wing4j.rrd.net.protocol;

import org.junit.Assert;
import org.junit.Test;
import org.wing4j.rrd.core.TableStatus;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Created by wing4j on 2017/8/4.
 */
public class RoundRobinIncreaseProtocolV1Test {

    @Test
    public void testConvert() throws Exception {
        RoundRobinIncreaseProtocolV1 format = new RoundRobinIncreaseProtocolV1();
        format.setTableName("table1");
        format.setColumn("response");
        format.setValue(1);
//        format.setNewValue(100);
        ByteBuffer buffer = format.convert();
//        System.out.println(HexUtils.toDisplayString(buffer.array()));
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        int messageType = buffer.getInt();
        Assert.assertEquals(MessageType.REQUEST.getCode(), messageType);
        RoundRobinIncreaseProtocolV1 format2 = new RoundRobinIncreaseProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals("table1", format2.getTableName());
        Assert.assertEquals("response", format2.getColumn());
        Assert.assertEquals(1, format2.getValue());
//        Assert.assertEquals(100, format2.getNewValue());
    }

    @Test
    public void testConvert1() throws Exception {
        RoundRobinIncreaseProtocolV1 format = new RoundRobinIncreaseProtocolV1();
        format.setTableName("table1");
        format.setColumn("response");
//        format.setValue(1);
        format.setNewValue(100);
        ByteBuffer buffer = format.convert();
//        System.out.println(HexUtils.toDisplayString(buffer.array()));
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        int messageType = buffer.getInt();
        Assert.assertEquals(MessageType.REQUEST.getCode(), messageType);
        RoundRobinIncreaseProtocolV1 format2 = new RoundRobinIncreaseProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals("table1", format2.getTableName());
        Assert.assertEquals("response", format2.getColumn());
//        Assert.assertEquals(1, format2.getValue());
        Assert.assertEquals(100, format2.getNewValue());
    }
}