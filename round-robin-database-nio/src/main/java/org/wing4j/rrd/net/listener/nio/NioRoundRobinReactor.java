package org.wing4j.rrd.net.listener.nio;

import lombok.Getter;
import org.wing4j.rrd.net.utils.SelectorUtil;
import org.wing4j.rrd.server.RoundRobinServer;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/10.
 * 反射器
 */
public class NioRoundRobinReactor implements Runnable {
    static Logger LOGGER = Logger.getLogger(NioRoundRobinReactor.class.getName());
    @Getter
    final String name;
    volatile Selector selector;
    final ConcurrentLinkedQueue<NioRoundRobinConnection> registerQueue;
    @Getter
    long reactCount;
    @Getter
    RoundRobinServer server;


    public NioRoundRobinReactor(String name, RoundRobinServer server) throws IOException {
        this.name = name;
        this.selector = Selector.open();
        this.registerQueue = new ConcurrentLinkedQueue<NioRoundRobinConnection>();
        this.server = server;
    }

    /**
     * 前置注册操作
     * @param c
     */
    final void postRegister(NioRoundRobinConnection c) {
        registerQueue.offer(c);
        selector.wakeup();
    }
    final void startup() {
        new Thread(this, name + "-reactor").start();
    }

    @Override
    public void run() {
        int invalidSelectCount = 0;

        for (; ; ) {
            ++reactCount;
            Set<SelectionKey> keys = null;
            try {
                final Selector tSelector = this.selector;
                long start = System.nanoTime();
                //在这里会阻塞500毫秒，进行死循环，但是监听器线程会调用postRegister方法，结束这里的阻塞；如果没有postRegister方法调用，则阻塞500毫秒
                tSelector.select(500L);
                long end = System.nanoTime();
                //注册选择器，将连接信息绑定到通道上
                register(tSelector);
                keys = tSelector.selectedKeys();
                if (keys.size() == 0 && (end - start) < server.getServerConfig().getMinSelectTimeInNanoSeconds()) {
                    invalidSelectCount++;
                } else {
                    invalidSelectCount = 0;
                    Iterator<SelectionKey> iterator = keys.iterator();
                    while (iterator.hasNext()){
                        SelectionKey key = iterator.next();
                        NioRoundRobinConnection con = null;
                        try {
                            //获取通道上绑定的连接信息
                            Object att = key.attachment();
                            //只要连接上，SelectionKey上就一定有连接信息
                            if (att != null) {
                                con = (NioRoundRobinConnection) att;
                                //如果通道可以读取，则进行异步读取
                                if (key.isValid() && key.isReadable()) {
                                    try {
                                        con.asyncRead();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                        con.close("program err:" + e.toString());
                                        continue;
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        LOGGER.warning("caught err:" + e);
                                        con.close("program err:" + e.toString());
                                        continue;
                                    }
                                }
                                //如果通道可以写，则进行写入
                                if (key.isValid() && key.isWritable()) {
                                    con.doNextWriteCheck();
                                }
                            } else {
                                key.cancel();
                            }
                        } catch (CancelledKeyException e) {
                            e.printStackTrace();
                            LOGGER.warning(con + " socket key canceled");
                        } catch (Exception e) {
                            e.printStackTrace();
                            LOGGER.warning(con + " " + e);
                        } catch (final Throwable e) {
                            // Catch exceptions such as OOM and close connection if exists,so that the reactor can keep running!
                            if (con != null) {
                                con.close("Bad: " + e);
                            }
                            e.printStackTrace();
                            LOGGER.warning("caught err: " + e);
                            continue;
                        }
                    }

                }
                if (invalidSelectCount > server.getServerConfig().getRebuildCountThreshold()) {
                    final Selector rebuildSelector = SelectorUtil.rebuildSelector(this.selector);
                    if (rebuildSelector != null) {
                        this.selector = rebuildSelector;
                    }
                    invalidSelectCount = 0;
                }
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.warning(name + e);
            } catch (final Throwable e) {
                // Catch exceptions such as OOM so that the reactor can keep running!
                e.printStackTrace();
                LOGGER.warning("caught err: " + e);
            } finally {
                if (keys != null) {
                    keys.clear();
                }

            }
        }
    }

    void register(Selector tSelector) {
        NioRoundRobinConnection c = null;
        if (registerQueue.isEmpty()) {
            return;
        }
        //处理注册的连接信息
        while ((c = registerQueue.poll()) != null) {
            try {
                //注册选择器到连接上
                c.getReadWriteHandler().register(tSelector);
                //进行异步读取
                c.register();
            } catch (Exception e) {
                c.close("register err" + e.toString());
            }
        }
    }
}
