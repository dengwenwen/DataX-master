package com.alibaba.datax.plugin.reader.hivereader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @auth: ronghua.yu
 * @time: 16/11/2
 * @desc:
 */
public class Test {
    private static String driverName =
            "org.apache.hive.jdbc.HiveDriver";

    private Connection conn;

    private Statement stmt;

    public void process () {
        try {
            Class.forName(driverName);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        try {
            conn = DriverManager.getConnection(
                    "jdbc:hive2://172.18.1.22:10000/yrh", "baseline", "baseline");
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        Test test = new Test();
        test.process();
    }
}