//package org.wing4j.rrd.aio.client;
//
//import java.io.IOException;
//import java.io.UnsupportedEncodingException;
//import java.nio.ByteBuffer;
//import java.nio.channels.AsynchronousSocketChannel;
//import java.nio.channels.CompletionHandler;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.Future;
//
//public class ReadHandler implements CompletionHandler<Integer, ByteBuffer> {
//    private AsynchronousSocketChannel clientChannel;
//    private CountDownLatch latch;
//    Future future;
//    public ReadHandler(AsynchronousSocketChannel clientChannel, CountDownLatch latch, Future future) {
//        this.clientChannel = clientChannel;
//        this.latch = latch;
//        this.future = future;
//    }
//
//    @Override
//    public void completed(Integer result, ByteBuffer buffer) {
//        buffer.flip();
//        byte[] bytes = new byte[buffer.remaining()];
//        buffer.get(bytes);
//        String body = null;
//        try {
//            body = new String(bytes, "UTF-8");
//            System.out.println("客户端收到结果:" + body);
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        }
//        ((Client.Future1)future).result = body;
//        synchronized (future) {
//            try {
//                Thread.currentThread().join();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    @Override
//    public void failed(Throwable exc, ByteBuffer attachment) {
//        System.err.println("数据读取失败...");
//        try {
//            clientChannel.close();
//            latch.countDown();
//        } catch (IOException e) {
//        }
//    }
//}