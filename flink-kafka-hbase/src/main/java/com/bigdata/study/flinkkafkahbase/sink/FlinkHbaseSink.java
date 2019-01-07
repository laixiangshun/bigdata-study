package com.bigdata.study.flinkkafkahbase.sink;

import com.bigdata.study.flinkkafkahbase.model.Metric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * Hbase sink
 **/
public class FlinkHbaseSink extends RichSinkFunction<Metric> {

    private static final String hbaseZookeeperQuorum = "192.168.20.48";
    private static final String hbaseZookeeperClinentPort = "2181";
    private static TableName hbaseTableName = TableName.valueOf("test");
    private static final String columnFamily = "cf";

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
        config.set("hbase.master", "10.45.151.26:60000");
        config.set("hbase.zookeeper.property.clientPort", hbaseZookeeperClinentPort);
        config.setInt("hbase.rpc.timeout", 20000);
        config.setInt("hbase.client.operation.timeout", 30000);
        config.setInt("hbase.client.scanner.timeout.period", 200000);
        connection = ConnectionFactory.createConnection(config);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(Metric value, Context context) throws Exception {
        Admin admin = connection.getAdmin();
        boolean tableExists = admin.tableExists(hbaseTableName);
        if (!tableExists) {
            admin.createTable(new HTableDescriptor(hbaseTableName).addFamily(new HColumnDescriptor(columnFamily)));
        }
        Table table = connection.getTable(hbaseTableName);
        long timeMillis = System.currentTimeMillis();
        Put put = new Put(Bytes.toBytes(timeMillis));
        Map<String, Object> fields = value.getFields();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            String field = entry.getKey();
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(field), Bytes.toBytes((String) entry.getValue()));
        }
        table.put(put);
        table.close();
        admin.close();
        connection.close();
    }
}
