package org.wing4j.rrd.impl;

import com.google.gson.Gson;
import org.junit.Test;
import org.wing4j.rrd.*;

import java.util.concurrent.TimeUnit;

/**
 * Created by liucheng on 2017/7/28.
 */
public class DefaultRoundRobinDatabaseTest {

    @Test
    public void testWrite() throws Exception {
        final RoundRobinDatabase database = DefaultRoundRobinDatabase.init();
        final RoundRobinConnection connection = database.open(1, TimeUnit.DAYS, "success", "fail", "request", "response", "other");
        connection.addTrigger(new RoundRobinTrigger() {
            @Override
            public String getName() {
                return "request";
            }

            @Override
            public boolean accept(int time, long data) {
                return data > 1000;
            }

            @Override
            public void trigger(int time, long data) {
                System.out.println("触发" + time + ","+ data);
            }
        });
        Thread[] threads = new Thread[200];
        for (int i = 0; i < 200; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < 2000; j++) {
                        connection.increase("success");
                    }
                    for (int j = 0; j < 2000; j++) {
                        connection.increase("fail");
                    }
                    for (int j = 0; j < 2000; j++) {
                        connection.increase("request");
                    }
                }
            };
        }
        for (Thread thrad : threads) {
            thrad.start();
        }
        Thread.sleep(60 * 1000);
        connection.persistent("D:/1.json");
        connection.close();
    }

    @Test
    public void testRead() throws Exception {
        RoundRobinDatabase database = DefaultRoundRobinDatabase.init();
        RoundRobinConnection connection = database.open("D:/1.json");
        long[][] data = connection.read("success", "request", "fail");
        String json = new Gson().toJson(data[0]);
        System.out.println(json);
        json = new Gson().toJson(data[1]);
        System.out.println(json);
        json = new Gson().toJson(data[2]);
        System.out.println(json);
        RoundRobinView view = connection.slice(20 * 60, "request");
        long[] data1 = view.read("request");
        System.out.println(data1.length);
        json = new Gson().toJson(data1);
        System.out.println(json);
        Thread.sleep(2 * 1000);

        connection.freezen();
        connection.addTrigger(new RoundRobinTrigger() {
            @Override
            public String getName() {
                return "request";
            }

            @Override
            public boolean accept(int time, long data) {
                return true;
            }

            @Override
            public void trigger(int time, long data) {
                System.out.println("触发");
            }
        });
        connection.merge(view, MergeType.ADD);
        connection.merge(view, MergeType.ADD);
        connection.merge(view, MergeType.ADD);
        connection.merge(view, MergeType.ADD);
//        connection.merge(view, (int)(System.currentTimeMillis() % (24 * 60 * 60)), MergeType.ADD);
//        connection.merge(view, (int)(System.currentTimeMillis() % (24 * 60 * 60)), MergeType.ADD);
//        connection.merge(view, (int)(System.currentTimeMillis() % (24 * 60 * 60)), MergeType.ADD);
        connection.unfreezen();
        json = new Gson().toJson(connection.slice(24 * 60 * 60, "request").read("request"));
        System.out.println(json);
        connection.close();
    }
}