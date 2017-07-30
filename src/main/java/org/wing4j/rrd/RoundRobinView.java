package org.wing4j.rrd;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.v1.RoundRobinFormatV1;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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

    public void write(OutputStream os) throws IOException {
        RoundRobinFormat format = new RoundRobinFormatV1(header, data, time, 1);
        format.write(os);
    }

}
