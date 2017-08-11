//package org.wing4j.rrd.aio.client;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.channels.AsynchronousSocketChannel;
//import java.nio.channels.CompletionHandler;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.Future;
//
//public class WriteHandler implements CompletionHandler<Integer, ByteBuffer> {
//    private AsynchronousSocketChannel clientChannel;
//    private CountDownLatch latch;
//    Future future;
//
//    public WriteHandler(AsynchronousSocketChannel clientChannel, CountDownLatch latch, Future future) {
//        this.clientChannel = clientChannel;
//        this.latch = latch;
//        this.future = future;
//    }
//
//    @Override
//    public void completed(Integer result, ByteBuffer buffer) {
//
//    }
//
//    @Override
//    public void failed(Throwable exc, ByteBuffer attachment) {
//        System.err.println("数据发送失败...");
//        try {
//            clientChannel.close();
//            latch.countDown();
//        } catch (IOException e) {
//        }
//    }
//}