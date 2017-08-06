package org.wing4j.rrd.server.aio.cmd;

import org.wing4j.rrd.*;
import org.wing4j.rrd.core.Table;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.net.protocol.*;
import org.wing4j.rrd.server.RoundRobinServerConfig;
import org.wing4j.rrd.server.authority.AuthorityService;
import org.wing4j.rrd.server.authority.impl.SimpleAuthorityService;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/6.
 * AIO分发器
 */
public class AioRoundRobinDispatcher {
    AuthorityService authorityService;

    public AioRoundRobinDispatcher(RoundRobinServerConfig serverConfig) {
        this.authorityService = new SimpleAuthorityService(serverConfig);
    }

    public ByteBuffer dispatch(ProtocolType protocolType, int version, ByteBuffer attachment, RoundRobinDatabase database) {
        ByteBuffer resultBuffer = ByteBuffer.wrap("unknown cmd!".getBytes());

        if (protocolType == ProtocolType.CONNECT && version == 1) {//打开数据库连接
            RoundRobinConnectProtocolV1 protocol = new RoundRobinConnectProtocolV1();
            protocol.convert(attachment);
            RoundRobinConnection connection = null;
            try {
                if (!this.authorityService.hasAuthority(protocol.getUsername(), protocol.getPassword())) {
                    throw new RoundRobinRuntimeException("无操作权限，可能是用户名或者密码不正确！");
                }
                //创建连接会话
                connection = database.open();
                protocol.setSessionId(connection.getSessionId());
            } catch (RoundRobinRuntimeException e) {
                protocol.setDesc(e.getMessage());
                protocol.setCode(RspCode.FAIL.getCode());
            } catch (Exception e) {
                protocol.setDesc("建立连接发生异常");
                protocol.setCode(RspCode.FAIL.getCode());
            }
            protocol.setMessageType(MessageType.RESPONSE);
            //写应答数据
            resultBuffer = protocol.convert();
        } else if (protocolType == ProtocolType.DIS_CONNECT && version == 1) {//关闭数据库连接
            RoundRobinDisConnectProtocolV1 protocol = new RoundRobinDisConnectProtocolV1();
            protocol.convert(attachment);
            RoundRobinConnection connection = null;
            try {
                //进行合并视图操作
                connection = database.getConnection(protocol.getSessionId());
            } catch (RoundRobinRuntimeException e) {
                protocol.setDesc(e.getMessage());
                protocol.setCode(RspCode.FAIL.getCode());
            } catch (Exception e) {
                protocol.setDesc("断开连接操作发生异常");
                protocol.setCode(RspCode.FAIL.getCode());
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            protocol.setMessageType(MessageType.RESPONSE);
            //写应答数据
            resultBuffer = protocol.convert();
        } else if (protocolType == ProtocolType.TABLE_METADATA && version == 1) {//获取数据数量
            //读取到数据流
            RoundRobinTableMetadataProtocolV1 protocol = new RoundRobinTableMetadataProtocolV1();
            protocol.convert(attachment);
            RoundRobinConnection connection = null;
            try {
                //进行合并视图操作
                connection = database.getConnection(protocol.getSessionId());
                TableMetadata metadata = connection.getTableMetadata(protocol.getTableName());
                protocol.setTableName(metadata.getName());
                protocol.setDataSize(metadata.getDataSize());
                protocol.setStatus(metadata.getStatus());
                protocol.setColumns(metadata.getColumns());
            } catch (RoundRobinRuntimeException e) {
                protocol.setDesc(e.getMessage());
                protocol.setCode(RspCode.FAIL.getCode());
            } catch (Exception e) {
                protocol.setDesc("获取表元信息发生异常");
                protocol.setCode(RspCode.FAIL.getCode());
            }
            protocol.setMessageType(MessageType.RESPONSE);
            //写应答数据
            resultBuffer = protocol.convert();
        } else if (protocolType == ProtocolType.SLICE && version == 1) {
            //读取到数据流
            RoundRobinSliceProtocolV1 protocol = new RoundRobinSliceProtocolV1();
            protocol.convert(attachment);
            RoundRobinConnection connection = null;
            try {
                //进行合并视图操作
                connection = database.getConnection(protocol.getSessionId());
                RoundRobinView view = connection.slice(protocol.getTableName(), protocol.getPos(), protocol.getSize(), protocol.getColumns());
                protocol.setPos(view.getTime());
                protocol.setResultSize(view.getData().length);
                protocol.setData(view.getData());
                protocol.setColumns(view.getMetadata().getColumns());
            } catch (RoundRobinRuntimeException e) {
                protocol.setDesc(e.getMessage());
                protocol.setCode(RspCode.FAIL.getCode());
            } catch (Exception e) {
                protocol.setDesc("视图切片操作发生异常");
                protocol.setCode(RspCode.FAIL.getCode());
            }
            //写应答数据
            protocol.setMessageType(MessageType.RESPONSE);
            //写应答数据
            resultBuffer = protocol.convert();
        } else if (protocolType == ProtocolType.QUERY_PAGE && version == 1) {
        } else if (protocolType == ProtocolType.INCREASE && version == 1) {
            //读取到数据流
            RoundRobinIncreaseProtocolV1 protocol = new RoundRobinIncreaseProtocolV1();
            protocol.convert(attachment);
            RoundRobinConnection connection = null;
            try {
                //进行合并视图操作
                connection = database.getConnection(protocol.getSessionId());
                long i = connection.increase(protocol.getTableName(), protocol.getColumn(), protocol.getPos(), protocol.getValue());
                protocol.setNewValue(i);
            } catch (RoundRobinRuntimeException e) {
                protocol.setDesc(e.getMessage());
                protocol.setCode(RspCode.FAIL.getCode());
            } catch (Exception e) {
                protocol.setDesc("自增字段操作发生异常");
                protocol.setCode(RspCode.FAIL.getCode());
            }
            protocol.setMessageType(MessageType.RESPONSE);
            //写应答数据
            resultBuffer = protocol.convert();
        } else if (protocolType == ProtocolType.CREATE_TABLE && version == 1) {
            //读取到数据流
            RoundRobinCreateTableProtocolV1 protocol = new RoundRobinCreateTableProtocolV1();
            protocol.convert(attachment);
            RoundRobinConnection connection = null;
            try {
                //进行合并视图操作
                connection = database.getConnection(protocol.getSessionId());
                //进行合并视图操作
                connection.createTable(protocol.getTableName(), protocol.getColumns());
            } catch (RoundRobinRuntimeException e) {
                protocol.setDesc(e.getMessage());
                protocol.setCode(RspCode.FAIL.getCode());
            } catch (Exception e) {
                protocol.setDesc("创建表结构发生错误");
                protocol.setCode(RspCode.FAIL.getCode());
            }
            protocol.setMessageType(MessageType.RESPONSE);
            //写应答数据
            resultBuffer = protocol.convert();
        } else if (protocolType == ProtocolType.DROP_TABLE && version == 1) {
            //读取到数据流
            RoundRobinDropTableProtocolV1 protocol = new RoundRobinDropTableProtocolV1();
            protocol.convert(attachment);
            RoundRobinConnection connection = null;
            try {
                //进行合并视图操作
                connection = database.getConnection(protocol.getSessionId());
                //进行合并视图操作
                connection.dropTable(protocol.getTableNames());
            } catch (RoundRobinRuntimeException e) {
                protocol.setDesc(e.getMessage());
                protocol.setCode(RspCode.FAIL.getCode());
            } catch (Exception e) {
                protocol.setDesc("删除表结构发生错误");
                protocol.setCode(RspCode.FAIL.getCode());
            }
            protocol.setMessageType(MessageType.RESPONSE);
            //写应答数据
            resultBuffer = protocol.convert();
        } else if (protocolType == ProtocolType.EXPAND && version == 1) {
            //读取到数据流
            RoundRobinExpandProtocolV1 protocol = new RoundRobinExpandProtocolV1();
            protocol.convert(attachment);
            RoundRobinConnection connection = null;
            try {
                //进行合并视图操作
                connection = database.getConnection(protocol.getSessionId());
                Table table = connection.expand(protocol.getTableName(), protocol.getColumns());
                protocol.setColumns(table.getMetadata().getColumns());
                protocol.setTableName(table.getMetadata().getName());
            } catch (RoundRobinRuntimeException e) {
                protocol.setDesc(e.getMessage());
                protocol.setCode(RspCode.FAIL.getCode());
            } catch (Exception e) {
                protocol.setDesc("字段扩容操作发生异常");
                protocol.setCode(RspCode.FAIL.getCode());
            }
            protocol.setMessageType(MessageType.RESPONSE);
            //写应答数据
            resultBuffer = protocol.convert();
        } else if (protocolType == ProtocolType.MERGE && version == 1) {
            //读取到数据流，构建格式对象
            RoundRobinMergeProtocolV1 protocol = new RoundRobinMergeProtocolV1();
            //通过格式对象，构建视图切片对象
            protocol.convert(attachment);
            RoundRobinView view = new RoundRobinView(protocol.getColumns(), protocol.getPos(), protocol.getData());
            RoundRobinConnection connection = null;
            try {
                //进行合并视图操作
                connection = database.getConnection(protocol.getSessionId());
                //进行合并视图操作
                RoundRobinView newView = connection.merge(protocol.getTableName(), protocol.getMergeType(), view);
                protocol.setColumns(newView.getMetadata().getColumns());
                protocol.setPos(newView.getTime());
                protocol.setData(newView.getData());
                connection.persistent(FormatType.CSV, 1);
                connection.close();
            } catch (RoundRobinRuntimeException e) {
                protocol.setDesc(e.getMessage());
                protocol.setCode(RspCode.FAIL.getCode());
            } catch (Exception e) {
                protocol.setDesc("合并视图操作发生异常");
                protocol.setCode(RspCode.FAIL.getCode());
            }
            protocol.setMessageType(MessageType.RESPONSE);
            //写应答数据
            resultBuffer = protocol.convert();
        } else {
            System.out.println("未知命令");
        }
        return resultBuffer;
    }
}
