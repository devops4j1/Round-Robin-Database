package org.wing4j.rrd;

import lombok.Data;

import java.util.Iterator;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
public class RoundRobinResultSet implements Iterable{
    String[] header;
    long[][] data;

    public RoundRobinResultSet(String[] header, long[][] data) {
        this.header = header;
        this.data = data;
    }

    /**
     * 获取数据
     * @param name 字段名
     * @return 数组
     */
    public long[] getData(String name){
        return data[getIndex(name)];
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
    public Iterator iterator() {
        return null;
    }
}
