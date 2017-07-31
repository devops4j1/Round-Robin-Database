package org.wing4j.rrd.format.csv.v1;

import org.junit.Test;
import org.wing4j.rrd.RoundRobinFormat;


/**
 * Created by wing4j on 2017/7/31.
 */
public class RoundRobinFormatCsvV1Test {
    @Test
    public void testWriteToFile() throws Exception {
        long[][] data = new long[1024][2];
        data[1023][1] = Long.MAX_VALUE;
        data[1022][1] = Long.MIN_VALUE;
        data[1021][1] = Long.MAX_VALUE / 2;
        String[] header = {"request"};
        RoundRobinFormat format = new RoundRobinFormatCsvV1(header, data, (int) (System.currentTimeMillis() / 1000));
        format.write("D:/1.csv");
    }

}