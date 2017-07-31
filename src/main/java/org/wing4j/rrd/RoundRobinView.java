package org.wing4j.rrd;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.core.format.bin.v1.RoundRobinFormatBinV1;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * Created by wing4j on 2017/7/30.
 */
@Data
@ToString
public class RoundRobinView {
    int time;
    /**
     *
     */
    String[] header;
    /**
     * 时间线
     */
    int[] timeline;
    /**
     * 数据节点
     */
    long[][] data;

    public RoundRobinView(String[] header, int[] timeline, long[][] data, int time) {
        this.header = header;
        this.timeline = timeline;
        this.data = data;
        this.time = time;
    }

    public int getIndex(String name) {
        int idx = 0;
        for (String name0 : header) {
            if (name.equals(name0)) {
                return idx;
            } else {
                idx++;
            }
        }
        throw new RuntimeException("未找到" + name);
    }

    public long[] read(String name){
        long[] data0 = new long[data.length];
        int idx = getIndex(name);
        for (int i = 0; i < data0.length; i++) {
            data0[i] = data[i][idx];
        }
        return data0;
    }

    /**
     * 将视图数据写入流，进行持久化或者传输
     * @param channel 通道
     * @throws IOException IO异常
     */
    public void write(WritableByteChannel channel) throws IOException {
        RoundRobinFormat format = new RoundRobinFormatBinV1(header, data, time);
        format.write(channel);
    }

}
