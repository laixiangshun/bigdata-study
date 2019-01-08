package com.bigdata.study.flinkjdbchbase.source;

import org.apache.commons.dbutils.DbUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * 自定义从mysql读取数据的source
 **/
public class JdbcSource extends RichSourceFunction<String> {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSource.class);

    private Connection connection;

    private PreparedStatement ps;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=utf8&useSSL=true", "root", "root");
        String sql = "select name from user";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            DbUtils.closeQuietly(connection);
        }
        if (ps != null) {
            DbUtils.close(ps);
        }
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        try {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                sourceContext.collect(name);
            }
        } catch (SQLException e) {
            logger.error("读取mysql数据出错：{}", e.getMessage());
        }
    }

    @Override
    public void cancel() {

    }
}
