package org.wing4j.rrd.core.format;

import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.RoundRobinRuntimeException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/5.
 */
public abstract class BaseRoundRobinFormat implements RoundRobinFormat{
    protected ByteBuffer put(ByteBuffer buffer, int data) {
        if (buffer.remaining() < 4) {
            //扩容
            ByteBuffer buffer1 = ByteBuffer.allocate(buffer.limit() + 1024);
            buffer.flip();
            buffer1.put(buffer);
            buffer = buffer1;
        }
        buffer.putInt(data);
        return buffer;
    }

    protected ByteBuffer put(ByteBuffer buffer, long data) {
        if (buffer.remaining() < 8) {
            //扩容
            ByteBuffer buffer1 = ByteBuffer.allocate(buffer.limit() + 1024);
            buffer.flip();
            buffer1.put(buffer);
            buffer = buffer1;
        }
        buffer.putLong(data);
        return buffer;
    }

    protected ByteBuffer put(ByteBuffer buffer, String data) {
        if (data == null) {
            data = "";
        }
        byte[] bytes = new byte[0];
        try {
            bytes = data.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RoundRobinRuntimeException("", e);
        }
        int len = bytes.length;
        if (buffer.remaining() < len) {
            //扩容
            ByteBuffer buffer1 = ByteBuffer.allocate(buffer.limit() + 1024);
            buffer.flip();
            buffer1.put(buffer);
            buffer = buffer1;
        }
        buffer.putInt(len);
        buffer.put(bytes);
        return buffer;
    }

    protected String get(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }
}
