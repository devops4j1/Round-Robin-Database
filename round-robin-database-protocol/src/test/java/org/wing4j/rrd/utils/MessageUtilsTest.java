package org.wing4j.rrd.utils;

import org.junit.Test;

/**
 * Created by wing4j on 2017/8/5.
 */
public class MessageUtilsTest {

    @Test
    public void testInt2bytes() throws Exception {
        byte[] data1 = new byte[]{0,0,0,(byte)0x2E};
        System.out.println(HexUtils.toDisplayString(data1));
        int size = MessageUtils.bytes2int(data1);
        System.out.println(size);
        System.out.println(HexUtils.toDisplayString(MessageUtils.int2bytes(46)));
    }
}