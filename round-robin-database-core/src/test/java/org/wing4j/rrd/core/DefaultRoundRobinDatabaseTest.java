package org.wing4j.rrd.core;

import org.junit.Test;
import org.wing4j.rrd.*;

/**
 * Created by liucheng on 2017/7/28.
 */
public class DefaultRoundRobinDatabaseTest {

    @Test
    public void testWrite() throws Exception {
        final RoundRobinDatabase database = new DefaultRoundRobinDatabase("default",new RoundRobinConfig());
        final RoundRobinConnection connection = database.open();
        connection.createTable("mo9", "success", "fail", "request", "response", "other");
        connection.registerTrigger("mo9", new RoundRobinTrigger() {
            @Override
            public String getName() {
                return "request";
            }

            @Override
            public boolean accept(int time, long data) {
                return data > 2000 * 200 - 1;
            }

            @Override
            public void trigger(int time, long data) {
                System.out.println("触发" + time + "," + data);
            }
        });
        Thread[] threads = new Thread[200];
        for (int i = 0; i < 200; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < 2000; j++) {
                        connection.increase("mo9", "success");
                    }
                    for (int j = 0; j < 2000; j++) {
                        connection.increase("mo9", "fail");
                    }
                    for (int j = 0; j < 2000; j++) {
                        connection.increase("mo9", "request");
                    }
                }
            };
        }
        for (Thread thrad : threads) {
            thrad.start();
        }
        Thread.sleep(2 * 1000);
        connection.persistent();
        connection.close();
    }

    @Test
    public void testRead() throws Exception {
        RoundRobinDatabase database = new DefaultRoundRobinDatabase("default",new RoundRobinConfig());
        RoundRobinConnection connection = database.open();
//        connection.persistent(FormatType.CSV, 1);
//        RoundRobinView view = connection.slice(5 * 60, "mo.request");
//        view.setColumns(new String[]{"mo2.request"});
//        connection.merge(view, MergeType.ADD);
        connection.persistent(FormatType.CSV, 1);

//        connection.persistent(FormatType.CSV, 1);
//        RoundRobinResultSet resultSet = connection.slice(connection.getColumns());
//        long[] data = resultSet.getData("mo9.request");
//        String json = new Gson().toJson(data);
//        System.out.println(json);
//        connection.persistent(FormatType.CSV, 1);
//        RoundRobinView view = connection.slice(5 * 60, "mo9.request");
//        RoundRobinResultSet resultSet1 = view.slice();
//        json = new Gson().toJson(resultSet1.getData("mo9.request"));
//        System.out.println(json);
//        Thread.sleep(2 * 1000);
//
//        connection.lock();
//        connection.addTrigger(new RoundRobinTrigger() {
//            @Override
//            public String getName() {
//                return "mo9.request";
//            }
//
//            @Override
//            public boolean accept(int time, long data) {
//                return true;
//            }
//
//            @Override
//            public void trigger(int time, long data) {
//                System.out.println("触发");
//            }
//        });
//        connection.merge(view, MergeType.ADD);
//        connection.merge(view, MergeType.ADD);
//        connection.merge(view, MergeType.ADD);
//        connection.merge(view, MergeType.ADD);
//        connection.merge(view, (int) (System.currentTimeMillis() % (24 * 60 * 60)), MergeType.ADD);
//        connection.merge(view, (int) (System.currentTimeMillis() % (24 * 60 * 60)), MergeType.ADD);
//        connection.merge(view, (int) (System.currentTimeMillis() % (24 * 60 * 60)), MergeType.ADD);
//        connection.unlock();
//        json = new Gson().toJson(connection.slice(24 * 60 * 60, "mo9.request").slice("mo9.request"));
//        System.out.println(json);
//        connection.close();
    }


    @Test
    public void testOpenTable() throws Exception {

    }

    @Test
    public void testDropTable() throws Exception {

    }

    @Test
    public void testExistTable() throws Exception {

    }

    @Test
    public void testListTable() throws Exception {

    }
}