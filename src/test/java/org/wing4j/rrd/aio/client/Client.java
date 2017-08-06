package org.wing4j.rrd.aio.client;

public class Client {
    private static String DEFAULT_HOST = "127.0.0.1";
    private static int DEFAULT_PORT = 12345;
    private static AsyncClientHandler clientHandle;

    public static void start() {
        start(DEFAULT_HOST, DEFAULT_PORT);
    }

    public static synchronized void start(String ip, int port) {
        if (clientHandle != null)
            return;
        clientHandle = new AsyncClientHandler(ip, port);
        new Thread(clientHandle, "Client").start();
    }
    //向服务器发送消息
    public static String sendMsg(String msg) throws Exception {
        return clientHandle.sendMsg(msg);
    }

    @SuppressWarnings("resource")
    public static void main(String[] args) throws Exception {
        Client.start();
        Thread.sleep(1 * 1000);
        for (int i = 0; i < 100; i++) {
            String msg = Client.sendMsg("1+1");
            System.out.println("msg:" + msg);
        }
        Thread.sleep(10 * 1000);
    }
}