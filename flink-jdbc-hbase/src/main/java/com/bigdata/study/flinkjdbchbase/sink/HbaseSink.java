package com.bigdata.study.flinkjdbchbase.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.shaded.org.joda.time.Instant;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * 自定义Hbase sink
 **/
public class HbaseSink extends ProcessFunction<String, String> {

    private String zookeeper;
    private String zkPort;
    private String tableName;
    private String family;
    private Table table;

    public HbaseSink(String zookeeper, String zkPort, String tableName, String family) {
        this.zookeeper = zookeeper;
        this.zkPort = zkPort;
        this.tableName = tableName;
        this.family = family;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, zookeeper);
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort);
        configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");
        configuration.setInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, 5000);
//        configuration.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY,5000);
        configuration.setInt(HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY, 5000);
        configuration.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 5000);
        configuration.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 5000);
        User user = User.create(UserGroupInformation.createRemoteUser("hbase"));
        Connection connection = ConnectionFactory.createConnection(configuration, user);
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            admin.createTable(new HTableDescriptor(TableName.valueOf(tableName)).addFamily(new HColumnDescriptor(family)));
        }
        table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (table != null) {
            table.close();
        }
    }

    @Override
    public void processElement(String s, Context context, Collector<String> collector) throws Exception {
        String rowKey = String.valueOf(Instant.now().getMillis());
        Put put = new Put(Bytes.toBytes(rowKey));
        put.setDurability(Durability.ASYNC_WAL);
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"), Bytes.toBytes(s));
        table.put(put);
    }
}
