package org.wing4j.rrd.net.connector.impl;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.net.format.RoundRobinFormatNetworkV1;
import org.wing4j.rrd.utils.HexUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
@ToString
public class BioRoundRobinConnector implements RoundRobinConnector {
    String address;
    int port;
    Socket socket;

    public BioRoundRobinConnector(String address, int port) throws IOException {
        this.address = address;
        this.port = port;
        this.socket = new Socket(address, port);
    }

    @Override
    public RoundRobinView read(int time, int size, String... names) {
        return null;
    }

    @Override
    public RoundRobinConnector write(RoundRobinView view, int time, MergeType mergeType) throws IOException {
        RoundRobinFormat format = new RoundRobinFormatNetworkV1(mergeType, time, view);
        ByteBuffer buffer = format.write();
        byte[] data = new byte[buffer.position()];
        buffer.flip();
        buffer.get(data);
        System.out.println(data.length);
        System.out.println(HexUtils.toDisplayString(data));
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] data11 = new byte[is.available()];
        is.read(data11);
        System.out.println(new String(data11));
        socket.close();
        return this;
    }

    @Override
    public RoundRobinConnector start() {
        return this;
    }

    @Override
    public RoundRobinConnector close() {
        return this;
    }
}
