package org.wing4j.rrd.impl;

import org.wing4j.rrd.RoundRobinConnection;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.v1.RoundRobinFormatV1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by wing4j on 2017/7/28.
 */
public class DefaultRoundRobinDatabase implements RoundRobinDatabase {
    Set<RoundRobinConnection> connections = new HashSet<>();
    static RoundRobinDatabase database;
    private DefaultRoundRobinDatabase() {
    }

    public static RoundRobinDatabase init(){
        if(database == null){
            synchronized (RoundRobinDatabase.class){
                if(database == null){
                    database = new DefaultRoundRobinDatabase();
                }
            }
        }
        return database;
    }

    boolean contain(String[] header, String name) {
        for (String name0 : header) {
            if (name.equals(name0)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RoundRobinConnection open(int time, TimeUnit timeUnit, String... names) {
        String[] header = new String[names.length + 1];
        if (contain(names, "index")) {
            throw new RuntimeException();
        }
        header[0] = "index";
        for (int i = 0; i < names.length; i++) {
            header[i + 1] = names[i];
        }
        int size = 0;
        if(timeUnit == TimeUnit.DAYS){
            size = time * 24 * 60 * 60;
        }else if(timeUnit == TimeUnit.HOURS){
            size = time * 60 * 60;
        }else if(timeUnit == TimeUnit.MINUTES){
            size = time * 60;
        }else{
            throw new RuntimeException("不支持的时间单位");
        }
        long[][] data = new long[size][header.length];
        for (int i = 0; i < size; i++) {
            long[] col = new long[header.length];
            col[0] = i;
            for (int j = 0; j < names.length; j++) {
                col[j + 1] = 0L;
            }
            data[i] = col;
        }
        RoundRobinConnection connection = new DefaultRoundRobinConnection(this, header, data, null);
        connections.add(connection);
        return connection;
    }
    @Override
    public RoundRobinConnection open(String fileName) throws IOException {
        //创建一个二进制文件
        RoundRobinFormat format = new RoundRobinFormatV1();
        format.readFormFile(fileName);
        //创建一个计时器，进行数据的异步写入
        RoundRobinConnection connection = new DefaultRoundRobinConnection(this, format.getHeader(), format.getData(), fileName);
        connections.add(connection);
        return connection;
    }

    @Override
    public void close() throws IOException {
        for (RoundRobinConnection connection : connections){
            connection.close();
            connections.remove(connection);
        }
    }

    @Override
    public void close(RoundRobinConnection connection) throws IOException {
        connections.remove(connection);
    }
}
