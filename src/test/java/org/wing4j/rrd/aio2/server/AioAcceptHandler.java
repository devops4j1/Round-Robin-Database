package org.wing4j.rrd.aio2.server;

import java.io.IOException;
import java.nio.ByteBuffer; 
import java.nio.channels.AsynchronousServerSocketChannel; 
import java.nio.channels.AsynchronousSocketChannel; 
import java.nio.channels.CompletionHandler; 
import java.util.concurrent.ExecutionException; 
import java.util.concurrent.Future; 
 
public class AioAcceptHandler implements CompletionHandler<AsynchronousSocketChannel, AsynchronousServerSocketChannel> {
    public void cancelled(AsynchronousServerSocketChannel attachment) { 
        System.out.println("cancelled"); 
    } 
 
    public void completed(AsynchronousSocketChannel socket, AsynchronousServerSocketChannel attachment) { 
        try { 
            attachment.accept(attachment, this);
            System.out.println(Thread.currentThread().getName() + " 有客户端连接"+ socket.getRemoteAddress().toString());
            ByteBuffer clientBuffer = ByteBuffer.allocate(1024);
            socket.read(clientBuffer, clientBuffer, new AioReadHandler(socket));
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
    } 
 
    public void failed(Throwable exc, AsynchronousServerSocketChannel attachment) { 
        exc.printStackTrace(); 
    }
}