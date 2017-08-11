package org.wing4j.rrd.utils;

/**
 * Created by wing4j on 2017/8/5.
 */
public class MessageUtils {
    public static long bytes2long(byte[] b) {
        long temp = 0;
        long res = 0;
        for (int i=0;i<8;i++) {
            res <<= 8;
            temp = b[i] & 0xff;
            res |= temp;
        }
        return res;
    }
    public static int bytes2int(byte[] bytes) {
        int num = bytes[3] & 0xFF;
        num |= ((bytes[2] << 8) & 0xFF00);
        num |= ((bytes[1] << 16) & 0xFF0000);
        num |= ((bytes[0] << 24) & 0xFF000000);
        return num;
    }
    public static byte[] int2bytes(int i) {
        byte[] b = new byte[4];

        b[3] = (byte) (0xff&i);
        b[2] = (byte) ((0xff00&i) >> 8);
        b[1] = (byte) ((0xff0000&i) >> 16);
        b[0] = (byte) ((0xff000000&i) >> 24);
        return b;
    }

    public static byte[] long2bytes(long num) {
        byte[] b = new byte[8];
        for (int i=0;i<8;i++) {
            b[i] = (byte)(num>>>(56-(i*8)));
        }
        return b;
    }
}
