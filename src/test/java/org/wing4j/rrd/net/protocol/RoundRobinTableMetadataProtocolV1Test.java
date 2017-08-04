package org.wing4j.rrd.net.protocol;

import org.junit.Assert;
import org.junit.Test;
import org.wing4j.rrd.*;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.core.TableStatus;
import org.wing4j.rrd.utils.HexUtils;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Created by 面试1 on 2017/8/4.
 */
public class RoundRobinTableMetadataProtocolV1Test {

    @Test
    public void testConvert() throws Exception {
        RoundRobinTableMetadataProtocolV1 format = new RoundRobinTableMetadataProtocolV1();
        format.setTableName("table1");
        format.setColumns(new String[]{"request", "response"});
        format.setFileName("D://table1.rrd");
        format.setDataSize(100);
        format.setStatus(TableStatus.NORMAL.ordinal());
        ByteBuffer buffer = format.convert();
//        System.out.println(HexUtils.toDisplayString(buffer.array()));
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        RoundRobinTableMetadataProtocolV1 format2 = new RoundRobinTableMetadataProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals("table1", format2.getTableName());
        Assert.assertEquals("request", format2.getColumns()[0]);
        Assert.assertEquals("response", format2.getColumns()[1]);
        Assert.assertEquals(100, format2.getDataSize());
        Assert.assertEquals(0, format2.getStatus());
    }

    @Test
    public void testConvert1() throws Exception {
        RoundRobinTableMetadataProtocolV1 format = new RoundRobinTableMetadataProtocolV1();
        format.setTableName("table1");
//        format.setColumns(new String[]{"request", "response"});
//        format.setFileName("D://table1.rrd");
//        format.setDataSize(100);
//        format.setStatus(TableStatus.NORMAL.ordinal());
        ByteBuffer buffer = format.convert();
//        System.out.println(HexUtils.toDisplayString(buffer.array()));
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        RoundRobinTableMetadataProtocolV1 format2 = new RoundRobinTableMetadataProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals("table1", format2.getTableName());
//        Assert.assertEquals("request", format2.getColumns()[0]);
//        Assert.assertEquals("response", format2.getColumns()[1]);
//        Assert.assertEquals(100, format2.getDataSize());
//        Assert.assertEquals(0, format2.getStatus());
    }
}