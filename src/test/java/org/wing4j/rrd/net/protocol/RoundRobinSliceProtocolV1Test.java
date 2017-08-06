package org.wing4j.rrd.net.protocol;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/4.
 */
public class RoundRobinSliceProtocolV1Test {

    @Test
    public void testConvert() throws Exception {
        RoundRobinSliceProtocolV1 format = new RoundRobinSliceProtocolV1();
        format.setTableName("table1");
        format.setMessageType(MessageType.RESPONSE);
        format.setColumns(new String[]{"request", "response"});
        format.setPos(3);
        format.setResultSize(2);
        long[][] data = new long[][]{
                {1, 2},
                {3, 4}
        };
        format.setData(data);
        format.setCode(RspCode.FAIL.getCode());
        format.setDesc(RspCode.FAIL.getDesc());
        ByteBuffer buffer = format.convert();
        buffer.flip();
        int size = buffer.getInt();
        int type = buffer.getInt();
        int version = buffer.getInt();
        int messageType = buffer.getInt();
        Assert.assertEquals(MessageType.RESPONSE.getCode(), messageType);
        RoundRobinSliceProtocolV1 format2 = new RoundRobinSliceProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals(RspCode.FAIL.getCode(), format2.getCode());
        Assert.assertEquals(RspCode.FAIL.getDesc(), format2.getDesc());
        Assert.assertEquals("table1", format2.getTableName());
        Assert.assertEquals("request", format2.getColumns()[0]);
        Assert.assertEquals("response", format2.getColumns()[1]);
        Assert.assertEquals(3, format2.getPos());
        Assert.assertEquals(2, format2.getResultSize());
        Assert.assertEquals(1, format2.getData()[0][0]);
        Assert.assertEquals(2, format2.getData()[0][1]);
        Assert.assertEquals(3, format2.getData()[1][0]);
        Assert.assertEquals(4, format2.getData()[1][1]);
    }

    @Test
    public void testConvert1() throws Exception {

    }
}