package org.wing4j.rrd.impl;


import org.wing4j.rrd.*;
import org.wing4j.rrd.format.bin.v1.RoundRobinFormatBinV1;
import org.wing4j.rrd.format.csv.v1.RoundRobinFormatCsvV1;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by wing4j on 2017/7/29.
 */
public class DefaultRoundRobinConnection implements RoundRobinConnection {
    /**
     * 状态
     */
    int status = Status.NORMAL;
    long[][] data = null;
    String[] header = null;
    volatile RoundRobinDatabase database;
    volatile String fileName;
    volatile List<RoundRobinTrigger>[] triggers;
    FormatType formatType = FormatType.BIN;
    /**
     * 任务执行线程池
     */
    volatile ScheduledExecutorService taskExecutor = Executors.newScheduledThreadPool(20);

    DefaultRoundRobinConnection(RoundRobinDatabase database, String[] header, long[][] data, String fileName) {
        this.database = database;
        this.header = header;
        this.data = data;
        this.fileName = fileName;
        this.triggers = new ArrayList[header.length];
        for (int i = 0; i < this.triggers.length; i++) {
            this.triggers[i] = new ArrayList<>();
        }
        if (fileName != null && fileName.trim().toLowerCase().endsWith(".csv")) {
            formatType = FormatType.CSV;
        }
    }

    @Override
    public RoundRobinConnection freezen() {
        if (status != Status.NORMAL) {
            throw new RuntimeException("数据库未进行冻结");
        }
        status = Status.FREEZEN;
        return this;
    }

    @Override
    public RoundRobinConnection unfreezen() {
        if (status != Status.FREEZEN) {
            throw new RuntimeException("数据库未进行冻结");
        }
        status = Status.NORMAL;
        return this;
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

    long[] read(String name) {
        long[] data0 = new long[data.length];
        int idx = getIndex(name);
        int current = getCurrent();
        for (int i = 0; i < data0.length; i++) {
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
    public RoundRobinConnection increase(final int sec, String name, int i) {
        final int idx = getIndex(name);
        synchronized (this.header[idx]) {
            this.data[sec][idx] = this.data[sec][idx] + i;
        }
        final long data0 = this.data[sec][idx];
        final List<RoundRobinTrigger> triggers = this.triggers[idx];
        taskExecutor.submit(new Runnable() {
            @Override
            public void run() {
                for (RoundRobinTrigger trigger : triggers) {
                    if (trigger.accept(sec, data0)) {
                        trigger.trigger(sec, data0);
                    }
                }
            }
        });
        return this;
    }

    @Override
    public RoundRobinView slice(int second, String... name) {
        long[][] data0 = new long[second][name.length];
        int[] timeline0 = new int[second];
        int[] indexes = new int[name.length];
        for (int i = 0; i < name.length; i++) {
            indexes[i] = getIndex(name[i]);
        }
        int pos = getCurrent();
        pos = pos - second;
        if (pos < 0) {
            pos = pos + DAY_SECOND;
        }
        for (int i = 0; i < second; i++) {
            for (int j = 0; j < name.length; j++) {
                int i0 = (pos + i) % DAY_SECOND;
                data0[i][j] = data[i0][indexes[j]];
            }
        }
        return new RoundRobinView(name, timeline0, data0, pos);
    }

    @Override
    public RoundRobinConnection addTrigger(RoundRobinTrigger trigger) {
        this.triggers[getIndex(trigger.getName())].add(trigger);
        return this;
    }

    @Override
    public RoundRobinConnection merge(RoundRobinView view, MergeType mergeType) {
        return merge(view, view.getTime(), mergeType);
    }

    @Override
    public RoundRobinConnection merge(RoundRobinView view, int time, MergeType mergeType) {
        if (status != Status.FREEZEN) {
            throw new RuntimeException("数据库未进行冻结");
        }
        long[][] data = null;
        String[] header = null;
        time = time - view.getData().length;
        data = view.getData();
        header = view.getHeader();
        for (String name : header) {
            int idx0 = view.getIndex(name);
            int idx1 = getIndex(name);
            for (int i = 0; i < data.length; i++) {
                if (mergeType == MergeType.REP) {
                    this.data[time + i][idx1] = data[i][idx0];
                } else if (mergeType == MergeType.ADD) {
                    this.data[time + i][idx1] = this.data[time + i][idx1] + data[i][idx0];
                } else if (mergeType == MergeType.SUB) {
                    this.data[time + i][idx1] = this.data[time + i][idx1] - data[i][idx0];
                } else if (mergeType == MergeType.AVG) {
                    this.data[time + i][idx1] = (this.data[time + i][idx1] + data[i][idx0]) / 2;
                }
            }
        }
        return this;
    }

    @Override
    public RoundRobinConnection persistent(FormatType formatType, int version) throws IOException {
        if (formatType == FormatType.BIN && version == 1) {
            RoundRobinFormat format = new RoundRobinFormatBinV1(header, data, getCurrent());
            format.write(fileName);
        } else if (formatType == FormatType.CSV && version == 1) {
            RoundRobinFormat format = new RoundRobinFormatCsvV1(header, data, getCurrent());
            format.write(fileName);
        } else {
            throw new RoundRobinRuntimeException("不支持的文件格式和文件版本");
        }
        //序列化数据
        return this;
    }

    public RoundRobinConnection persistent() throws IOException {
        return persistent(formatType, 1);
    }

    /**
     * 异步写入
     *
     * @return
     */
    void asyncWrite() {
        //创建一个定时器，进行定期调用写入数据
    }


    @Override
    public InputStream toStream() {
        return null;
    }

    @Override
    public void close() throws IOException {
        if (database.getConfig().isAutoPersistent()) {
            persistent();
        }
        database.close(this);
    }

    public int getCurrent() {
        return ((int) (System.currentTimeMillis() / 1000) + (8 * 60 * 60)) % DAY_SECOND;
    }
}
