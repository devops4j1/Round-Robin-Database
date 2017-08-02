package org.wing4j.rrd.aio2.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class AioReadHandler implements CompletionHandler<Integer, ByteBuffer> {
    private AsynchronousSocketChannel channel;

    public AioReadHandler(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    private CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    public void completed(Integer i, ByteBuffer buf) {
        if (i > 0) {
            try {
                System.out.println("position:" + buf.position() + " limit: " + buf.limit());
                System.out.println(buf);
                buf.flip();
                buf.compact();
                System.out.println(Thread.currentThread().getName() + " read finish" + "收到" + channel.getRemoteAddress().toString() + "的消息:" + decoder.decode(buf));
            } catch (CharacterCodingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (i == -1) {
            try {
                System.out.println("对端断线:" + channel.getRemoteAddress().toString());
                buf.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println(Thread.currentThread().getName() + " " + i);
        }

    }

    public void failed(Throwable exc, ByteBuffer buf) {
        System.out.println(exc);
    }
}