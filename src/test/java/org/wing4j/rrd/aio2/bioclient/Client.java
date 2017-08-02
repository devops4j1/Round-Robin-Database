package org.wing4j.rrd.aio2.bioclient;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by 面试1 on 2017/8/1.
 */
public class Client {
    public static void main(String[] args) throws IOException {
        for (int i = 0; i < 100000; i++) {
            Socket socket = new Socket("localhost", 9008);
            socket.getOutputStream().write("1".getBytes());
            byte[] data = new byte[1024];
            socket.getInputStream().read(data);
            System.out.println(new String(data).trim());
        }
    }
}
