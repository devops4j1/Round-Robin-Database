package org.wing4j.rrd.impl;


import org.wing4j.rrd.RoundRobinConnection;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.v1.RoundRobinFormatV1;

import java.io.*;

/**
 * Created by wing4j on 2017/7/29.
 */
public class DefaultRoundRobinConnection implements RoundRobinConnection {
    long[][] data = null;
    String[] header = null;
    volatile RoundRobinDatabase database;
    volatile String fileName;

    DefaultRoundRobinConnection(RoundRobinDatabase database, String[] header, long[][] data) {
        this.database = database;
        this.header = header;
        this.data = data;
    }

    @Override
    public RoundRobinDatabase getDatabase() {
        return database;
    }

    @Override
    public String[] getHeader() {
        return header;
    }

    @Override
    public long[][] read(String... name) {
        long[][] data0 = new long[name.length][data.length];
        for (int i = 0; i < name.length; i++) {
            data0[i] = read(name[i]);
        }
        return data0;
    }

    long[] read(String name){
        long[] data0 = new long[data.length];
        int idx = getIndex(name);
        int current = getCurrent();
        for (int i = 0; i < data0.length; i++){
            data0[i] = data[(i + current) % data0.length][idx];
        }
        return data0;
    }

    @Override
    public boolean contain(String name) {
        for (String name0 : header) {
            if (name.equals(name0)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RoundRobinConnection increase(int sec, String name) {
        return increase(sec, name, 1);
    }

    int getIndex(String name) {
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

    @Override
    public RoundRobinConnection increase(String name) {
        int sec = getCurrent();
        return increase(sec, name, 1);
    }

    @Override
    public RoundRobinConnection increase(String name, int i) {
        int sec = getCurrent();
        return increase(sec, name, i);
    }

    @Override
    public RoundRobinConnection increase(int sec, String name, int i) {
        int idx = getIndex(name);
        synchronized (this.header[idx]) {
            this.data[sec][idx] = this.data[sec][idx] + i;
        }
        return this;
    }

    @Override
    public RoundRobinConnection persistent(String fileName) throws IOException {
        this.fileName = fileName;
        RoundRobinFormat format = new RoundRobinFormatV1(header, data, getCurrent(), 1);
        format.writeToFile(fileName);
        //序列化数据
        return this;
    }

    /**
     * 异步写入
     * @return
     */
    void asyncWrite(){
        //创建一个定时器，进行定期调用写入数据
    }


    @Override
    public InputStream toStream() {
        return null;
    }

    @Override
    public void close() throws IOException {
        persistent(this.fileName);
        database.close(this);
    }

    public int getCurrent(){
        return ((int) (System.currentTimeMillis() / 1000) + (8 * 60 * 60)) % (24 * 60 * 60);
    }
}
