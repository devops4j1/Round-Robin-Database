package org.wing4j.rrd.core.format.bin.v1;

import org.junit.Assert;
import org.junit.Test;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.utils.HexUtils;

import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/7/29.
 */
public class RoundRobinFormatBinV1Test {

    @Test
    public void testWriteToFile() throws Exception {
        long[][] data = new long[4][2];
        data[1][1] = Long.MAX_VALUE;
        data[2][1] = Long.MIN_VALUE;
        data[3][1] = Long.MAX_VALUE / 2;
        String[] header = {"request", "response"};
        RoundRobinFormat format = new RoundRobinFormatBinV1("default","TABLE1", header, data, 4);
        format.write("D:/1.rrd");
    }

    @Test
    public void testWriteToBuffer() throws Exception {
        long[][] data = new long[4][2];
        data[1][1] = Long.MAX_VALUE;
        data[2][1] = Long.MIN_VALUE;
        data[3][1] = Long.MAX_VALUE / 2;
        String[] header = {"request", "response"};
        RoundRobinFormat format = new RoundRobinFormatBinV1("default","TABLE1", header, data, 4);
        ByteBuffer buffer = null;
        buffer = format.write(buffer);
        buffer.flip();
    }

    @Test
    public void testReadFormFile() throws Exception {
        RoundRobinFormatBinV1 format = new RoundRobinFormatBinV1();
        format.read("D:/1.rrd");
        Assert.assertEquals(Long.MAX_VALUE, format.getData()[1][1]);
        Assert.assertEquals(Long.MIN_VALUE, format.getData()[2][1]);
        Assert.assertEquals(Long.MAX_VALUE / 2, format.getData()[3][1]);
    }
}