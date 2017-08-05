package org.wing4j.rrd.core.format;

import org.junit.Test;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.core.format.bin.v1.RoundRobinFormatBinV1;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Created by 面试1 on 2017/8/5.
 */
public class RoundRobinFormatLoaderTest {

    @Test
    public void testLoad() throws Exception {
        RoundRobinFormatLoader loader = new RoundRobinFormatLoader(new File("D:/1.rrd"));
        RoundRobinFormat format = loader.load();
        System.out.println(format);
    }

    @Test
    public void testLoad2() throws Exception {
        long[][] data = new long[4][2];
        data[1][1] = Long.MAX_VALUE;
        data[2][1] = Long.MIN_VALUE;
        data[3][1] = Long.MAX_VALUE / 2;
        String[] header = {"request", "response"};
        RoundRobinFormat format = new RoundRobinFormatBinV1("default","TABLE1", header, data, 4);
        RoundRobinFormatLoader loader = new RoundRobinFormatLoader(null);
        ByteBuffer buffer = format.write();
        buffer.flip();
        RoundRobinFormat format2 = loader.load(buffer);
        System.out.println(format2);
    }
}