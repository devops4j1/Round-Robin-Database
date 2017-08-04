package org.wing4j.rrd.core.format.net;

import org.junit.Assert;
import org.junit.Test;
import org.wing4j.rrd.*;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.core.format.net.RoundRobinFormatNetworkV1;
import org.wing4j.rrd.utils.HexUtils;

import java.nio.ByteBuffer;

/**
 * Created by 面试1 on 2017/8/4.
 */
public class RoundRobinFormatNetworkV1Test {

    @Test
    public void testRead() throws Exception {
        RoundRobinConfig config = new RoundRobinConfig();
        RoundRobinDatabase database = DefaultRoundRobinDatabase.init(config);
        RoundRobinConnection connection = database.open();
        RoundRobinView view = connection.slice("mo9", 1 * 60, "request");
        RoundRobinFormat format = new RoundRobinFormatNetworkV1(MergeType.ADD, view.getTime(), "mo", view);
        ByteBuffer buffer = format.write();
        System.out.println(HexUtils.toDisplayString(buffer.array()));
        buffer.flip();
        int size = buffer.getInt();
        RoundRobinFormat format1 = new RoundRobinFormatNetworkV1(buffer);
        Assert.assertEquals("mo", format1.getTableName());
        Assert.assertEquals("request", format1.getColumns()[0]);
    }

    @Test
    public void testWrite() throws Exception {
        RoundRobinConfig config = new RoundRobinConfig();
        RoundRobinDatabase database = DefaultRoundRobinDatabase.init(config);
        RoundRobinConnection connection = database.open();
        RoundRobinView view = connection.slice("mo9", 1 * 60, "request");
        RoundRobinFormat format = new RoundRobinFormatNetworkV1(MergeType.ADD, view.getTime(), "mo", view);
        ByteBuffer buffer = format.write();
        System.out.println(HexUtils.toDisplayString(buffer.array()));
    }
}