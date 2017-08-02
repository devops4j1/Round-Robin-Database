package org.wing4j.rrd.aio2.server;

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
 
    public void cancelled(ByteBuffer attachment) { 
        System.out.println("cancelled"); 
    } 
 
    private CharsetDecoder decoder = Charset.forName("GBK").newDecoder(); 
 
    public void completed(Integer i, ByteBuffer buf) { 
        if (i > 0) { 
            buf.flip(); 
            try { 
                System.out.println(Thread.currentThread().getName() + "收到" + channel.getRemoteAddress().toString() + "的消息:" + decoder.decode(buf));
                buf.compact(); 
            } catch (CharacterCodingException e) { 
                e.printStackTrace(); 
            } catch (IOException e) { 
                e.printStackTrace(); 
            }
            buf.clear();
            ByteBuffer buffer = ByteBuffer.wrap((Thread.currentThread().getName() + " OK").getBytes());
            channel.write(buffer, buffer, new AioWriteHandler(channel));
        } else if (i == -1) { 
            try {
                System.out.println(Thread.currentThread().getName() + "客户端断线:" + channel.getRemoteAddress().toString());
                buf.clear();
                buf = null;
            } catch (IOException e) { 
                e.printStackTrace(); 
            } 
        } 
    } 
 
    public void failed(Throwable exc, ByteBuffer buf) { 
        System.out.println(exc); 
    } 
}