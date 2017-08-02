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
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
@ToString
public class AioRoundRobinConnector implements RoundRobinConnector {
    String address;
    int port;
    Socket socket;

    public AioRoundRobinConnector(String address, int port) throws IOException {
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
        buffer.flip();
        System.out.println(buffer.remaining());
        System.out.println(HexUtils.toDisplayString(buffer.array()));
        //按照接受的服务器读取的缓存量进行写操作
        while (buffer.hasRemaining()){
            socket.getChannel().write(buffer);
        }
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
