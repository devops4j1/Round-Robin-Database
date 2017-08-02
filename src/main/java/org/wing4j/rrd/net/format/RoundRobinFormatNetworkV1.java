package org.wing4j.rrd.net.format;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.RoundRobinRuntimeException;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.format.bin.v1.RoundRobinFormatBinV1;
import org.wing4j.rrd.utils.HexUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/2.
 */
public class RoundRobinFormatNetworkV1 implements RoundRobinFormat{
    MergeType mergeType;
    int version = 1;
    int current = 0;
    RoundRobinFormat format;
    static final boolean DEBUG = true;
    public RoundRobinFormatNetworkV1(MergeType mergeType, int current, RoundRobinView view) {
        this.mergeType = mergeType;
        this.current = current;
        format = new RoundRobinFormatBinV1(view.getHeader(), view.getData(), view.getTime());
    }
    public RoundRobinFormatNetworkV1() {

    }
    public RoundRobinFormatNetworkV1(MergeType mergeType, int current, String[] header, long[][] data) {
        this.mergeType = mergeType;
        this.current = current;
        format = new RoundRobinFormatBinV1(header, data, current);
    }

    @Override
    public long[][] getData() {
        return format.getData();
    }

    @Override
    public void setData(long[][] data) {
        format.setData(data);
    }

    @Override
    public String[] getHeader() {
        return format.getHeader();
    }

    @Override
    public void setHeader(String[] header) {
        format.setHeader(header);
    }

    @Override
    public int getCurrent() {
        return current;
    }

    @Override
    public void setCurrent(int current) {
        this.current = current;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public void read(String fileName) throws IOException {
        throw new RoundRobinRuntimeException("未实现");
    }

    @Override
    public void write(String fileName) throws IOException {
        throw new RoundRobinRuntimeException("未实现");
    }

    @Override
    public void read(ByteBuffer buffer) {
        int type = buffer.getInt();
        this.mergeType = MergeType.values()[type];
        this.current = buffer.getInt();
        format = new RoundRobinFormatBinV1();
        format.read(buffer);
    }

    @Override
    public ByteBuffer write() {
        return write(ByteBuffer.allocate(5 * 1024));
    }

    @Override
    public ByteBuffer write(ByteBuffer buffer) {
        //网络传输协议
        //int 整个报文长度
        buffer.putInt(0);
        //int 合并模式
        buffer.putInt(mergeType.ordinal());
        //int 合并到的截至时间点
        buffer.putInt(current);
        //--与切片试图格式一直
        ByteBuffer byteBuffer = format.write();
        byteBuffer.flip();
        if(buffer.remaining() < byteBuffer.limit()){
            //扩容
            ByteBuffer buffer1 = ByteBuffer.allocate(buffer.limit() + byteBuffer.limit());
            buffer.flip();
            buffer1.put(buffer);
            buffer = buffer1;
        }
        buffer.put(byteBuffer);
        //将报文总长度回填到第一个字节
        buffer.putInt(0,buffer.position() - 4);
        if(DEBUG){
            System.out.println(HexUtils.toDisplayString(buffer.array()));
        }
        return buffer;
    }

    public MergeType getMergeType() {
        return mergeType;
    }
}
