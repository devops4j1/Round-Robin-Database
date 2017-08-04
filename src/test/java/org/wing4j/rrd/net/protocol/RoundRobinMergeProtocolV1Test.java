package org.wing4j.rrd.net.protocol;

import org.junit.Assert;
import org.junit.Test;
import org.wing4j.rrd.*;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.utils.HexUtils;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Created by 面试1 on 2017/8/4.
 */
public class RoundRobinMergeProtocolV1Test {
    @Test
    public void testRead() throws Exception {
        RoundRobinConfig config = new RoundRobinConfig();
        RoundRobinDatabase database = DefaultRoundRobinDatabase.init(config);
        RoundRobinConnection connection = database.open();
        RoundRobinView view = connection.slice("mo9", 1 * 60, "request");
        RoundRobinMergeProtocolV1 format = new RoundRobinMergeProtocolV1();
        format.setData(view.getData());
        format.setColumns(new String[]{"request"});
        format.setCurrent(view.getTime());
        format.setMergeType(MergeType.ADD);
        format.setTableName("mo");
        ByteBuffer buffer = format.convert();
        System.out.println(HexUtils.toDisplayString(buffer.array()));
        buffer.flip();
        int size = buffer.getInt();
        RoundRobinMergeProtocolV1 format2 = new RoundRobinMergeProtocolV1();
        format2.convert(buffer);
        Assert.assertEquals("mo", format2.getTableName());
        Assert.assertEquals("request", format2.getColumns()[0]);
    }

    @Test
    public void testWrite() throws Exception {
        RoundRobinConfig config = new RoundRobinConfig();
        RoundRobinDatabase database = DefaultRoundRobinDatabase.init(config);
        RoundRobinConnection connection = database.open();
        RoundRobinView view = connection.slice("mo9", 1 * 60, "request");
        RoundRobinProtocol format = new RoundRobinMergeProtocolV1();
        ByteBuffer buffer = format.convert();
        System.out.println(HexUtils.toDisplayString(buffer.array()));
    }
}