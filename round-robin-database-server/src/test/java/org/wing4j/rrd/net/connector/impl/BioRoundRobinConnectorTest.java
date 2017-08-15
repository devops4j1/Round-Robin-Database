package org.wing4j.rrd.net.connector.impl;

import org.junit.Assert;
import org.junit.Test;
import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.net.connector.RoundRobinConnector;

/**
 * Created by wing4j on 2017/8/2.
 */
public class BioRoundRobinConnectorTest {

//    String address = "interface-platform-manager.d1.jr-zbj.com";
    String address = "127.0.0.1";

    @Test
    public void testMerge() throws Exception {
        RoundRobinConnector connector = new BioRoundRobinConnector(null, null, address, 8099);
        String sessionId = connector.connect("admin", "password");
        try {
            connector.dropTable("mo9");
        }catch (Exception e){

        }
        connector.createTable("mo9", "request", "response");
        for (int i = 0; i < 100; i++) {
            long[][] data = new long[][]{
                    {1, 2},
                    {3, 4}
            };
            RoundRobinView view = new RoundRobinView(new String[]{"request", "response"}, 2, data);
            RoundRobinView newView = connector.merge("mo9", MergeType.ADD, view, 2);
            System.out.println(newView);
        }
    }

    @Test
    public void testGetTableMetadata() throws Exception {
        RoundRobinConnector connector = new BioRoundRobinConnector(null, null, address, 8099);
        connector.connect("admin", "password");
//        try {
//            connector.dropTable("mo9");
//        }catch (Exception e){
//
//        }
//        connector.createTable("mo9", "request", "response");
        for (int i = 0; i < 10; i++) {
            TableMetadata metadata = connector.getTableMetadata("mo9");
            System.out.println(metadata);
        }
    }

    @Test
    public void testIncrease() throws Exception {
        RoundRobinConnector connector = new BioRoundRobinConnector(null, null, address, 8099);
        connector.connect("admin", "password");
        try {
            connector.dropTable("mo9");
        }catch (Exception e){

        }
        connector.createTable("mo9", "request", "response");
        for (int i = 0; i < 1000; i++) {
            try {
                long v = connector.increase("mo9", "request", i, i);
                System.out.println(v);
            }catch (Exception e){
                System.out.println("----------" + i);
            }
        }

    }

    @Test
    public void testExpand() throws Exception {
        RoundRobinConnector connector = new BioRoundRobinConnector(null, null, address, 8099);
        connector.connect("admin", "password");
//        try {
//            connector.dropTable("mo9");
//        }catch (Exception e){
//
//        }
//        connector.createTable("mo9", "request", "response");
        TableMetadata table = connector.expand("mo9", "rep", "other1");
        System.out.println(table);
    }

    @Test
    public void testCreateTable() throws Exception {
        RoundRobinConnector connector = new BioRoundRobinConnector(null, null, address, 8099);
        connector.connect("admin", "password");
        try {
            connector.dropTable("mo9");
        }catch (Exception e){

        }
        connector.createTable("mo9", "request", "response");
    }

    @Test
    public void testSlice() throws Exception {
        RoundRobinConnector connector = new BioRoundRobinConnector(null, null, address, 8099);
        connector.connect("admin", "password");
//        try {
//            connector.dropTable("mo9");
//        }catch (Exception e){
//
//        }
//        connector.createTable("mo9", "request", "response");
        for (int i = 0; i < 1; i++) {
            RoundRobinView view = connector.slice("mo9", 100, 100, "request");
            System.out.println(view);
        }

    }

    @Test
    public void testConnect() throws Exception {
        RoundRobinConnector connector = new BioRoundRobinConnector(null, null, address, 8099);
        String sessionId = connector.connect("admin", "password");
        connector.connect("admin", "password");
        connector.connect("admin", "password");
        System.out.println(sessionId);
//        connector.disconnect("73bc9ddb68314294bbee8dea96b82371");
    }

    @Test
    public void testPersistentTable() throws Exception {
        RoundRobinConnector connector = new BioRoundRobinConnector(null, null, address, 8099);
        connector.connect("admin", "password");
//        try {
//            connector.dropTable("mo9");
//        }catch (Exception e){
//
//        }
//        connector.createTable("mo9", "request", "response");
        connector.persistentTable(0, FormatType.CSV, 1,  "mo9");
    }

    @Test
    public void testPersistentTable1() throws Exception {
        RoundRobinConnector connector = new BioRoundRobinConnector(null, null, address, 8099);
        connector.connect("admin", "password");
        try {
            connector.dropTable("mo9");
        }catch (Exception e){

        }
        connector.createTable("mo9", "request", "response");
        connector.persistentTable(0, FormatType.BIN, 1,  "mo9");
    }

    @Test
    public void testSet() throws Exception {
        RoundRobinConnector connector = new BioRoundRobinConnector(null, null, address, 8099);
        connector.connect("admin", "password");
//        try {
//            connector.dropTable("mo9");
//        }catch (Exception e){
//
//        }
//        connector.createTable("mo9", "request", "response");
        long val = connector.set("mo9", "request", 2, 123);
//        System.out.println(val);
    }

    @Test
    public void testGet() throws Exception {
        Thread[] threads = new Thread[100];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(){
                @Override
                public void run() {

                    try {
                        RoundRobinConnector connector = new BioRoundRobinConnector(null, null, address, 8099);
                        connector.connect("admin", "password");
                        for (int i = 0; i < 1000; i++) {
                            long val2 = connector.set("mo9", "request", 0, i);
//                            System.out.println(val2);
                            Assert.assertEquals(i, val2);
                            long val3 = connector.get("mo9", "request", 0);
//                            System.out.println(val3);
                            Assert.assertEquals(i, val2);
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            };
        }
        for (Thread t : threads){
            t.start();
        }

        Thread.sleep(120 * 1000);
    }
}