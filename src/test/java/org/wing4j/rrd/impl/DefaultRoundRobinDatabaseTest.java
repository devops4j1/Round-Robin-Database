package org.wing4j.rrd.impl;

import com.google.gson.Gson;
import org.junit.Test;
import org.wing4j.rrd.RoundRobinConnection;
import org.wing4j.rrd.RoundRobinDatabase;

import java.util.concurrent.TimeUnit;

/**
 * Created by liucheng on 2017/7/28.
 */
public class DefaultRoundRobinDatabaseTest {

    @Test
    public void testWrite() throws Exception {
        final RoundRobinDatabase database = DefaultRoundRobinDatabase.init();
        final RoundRobinConnection connection = database.open(1, TimeUnit.DAYS, "success", "fail", "request", "response", "other");
        Thread[] threads = new Thread[900];
        for (int i = 0; i < 900; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < 10; j++) {
                        connection.increase("success");
                    }
                    for (int j = 0; j < 3000; j++) {
                        connection.increase("fail");
                    }
                    for (int j = 0; j < 10; j++) {
                        connection.increase("request");
                    }
                }
            };
        }
        for (Thread thrad : threads) {
            thrad.start();
        }
        connection.persistent("D:/1.json");
        Thread.sleep(1 * 1000);
        connection.close();
    }

    @Test
    public void testRead() throws Exception {
        RoundRobinDatabase database = DefaultRoundRobinDatabase.init();
        RoundRobinConnection connection = database.open("D:/1.json");
        long[][] data = connection.read("success","request");
        String json = new Gson().toJson(data[0]);
        System.out.println(json);
        json = new Gson().toJson(data[1]);
        System.out.println(json);
        connection.persistent("D:/2.json");
    }
}