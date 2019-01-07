package com.bigdata.study.flinkkafkahbase.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Hbase source
 **/
public class FlinkHbaseSource extends RichSourceFunction<Map<String, String>> {

    private static final String hbaseZookeeperQuorum = "192.168.20.48";
    private static final String hbaseZookeeperClinentPort = "2181";
    private static TableName hbaseTableName = TableName.valueOf("test");
    private static final String columnFamily = "cf";

    private static final ObjectMapper mapper = new ObjectMapper();

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
    public void run(SourceContext<Map<String, String>> sourceContext) throws Exception {
        Table table = connection.getTable(hbaseTableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(columnFamily));
        ResultScanner tableScanner = table.getScanner(scan);
        tableScanner.iterator().forEachRemaining(scanner -> {
            Cell[] cells = scanner.rawCells();
            Map<String, String> map = new HashMap<>();
            for (Cell cell : cells) {
                String key = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                map.put(key, value);
            }
            sourceContext.collect(map);
        });

    }

    @Override
    public void cancel() {
        if (!connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
