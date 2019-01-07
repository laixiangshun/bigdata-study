package com.bigdata.study.sparkphoenix.utils;

import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * @Description
 * @Author hasee
 * @Date 2019/1/3
 **/
public class PhoenixUtil {

    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection() {
        try {
            if (CollectionUtils.isEmpty(connectionQueue)) {
                connectionQueue = new LinkedList<>();
                for (int i = 0; i < 3; i++) {
                    Connection connection = DriverManager.getConnection("jdbc:phoenix:zk:2181");
                    connectionQueue.add(connection);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    public static void returnConnection(Connection connection) {
        connectionQueue.push(connection);
    }
}
