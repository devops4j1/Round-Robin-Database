package org.wing4j.rrd.core.format.bin.v1;

import org.junit.Assert;
import org.junit.Test;
import org.wing4j.rrd.RoundRobinFormat;

/**
 * Created by wing4j on 2017/7/29.
 */
public class RoundRobinFormatBinV1Test {

    @Test
    public void testWriteToFile() throws Exception {
        long[][] data = new long[1024][2];
        data[1023][1] = Long.MAX_VALUE;
        data[1022][1] = Long.MIN_VALUE;
        data[1021][1] = Long.MAX_VALUE / 2;
        String[] header = {"request"};
        RoundRobinFormat format = new RoundRobinFormatBinV1(header, data, (int) (System.currentTimeMillis() / 1000));
        format.write("D:/1.rrd");
    }


    @Test
    public void testReadFormFile() throws Exception {
        RoundRobinFormatBinV1 format = new RoundRobinFormatBinV1();
        format.read("D:/1.rrd");
        Assert.assertEquals(Long.MAX_VALUE, format.getData()[1023][1]);
        Assert.assertEquals(Long.MIN_VALUE, format.getData()[1022][1]);
        Assert.assertEquals(Long.MAX_VALUE / 2, format.getData()[1021][1]);
    }
}