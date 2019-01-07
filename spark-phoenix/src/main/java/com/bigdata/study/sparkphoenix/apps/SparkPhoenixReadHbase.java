package com.bigdata.study.sparkphoenix.apps;

import com.bigdata.study.sparkphoenix.utils.PhoenixUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * spark 通过phoenix读取Hbase数据
 **/
public class SparkPhoenixReadHbase {

    public static void main(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "zk");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase");
        configuration.set(TableInputFormat.INPUT_TABLE, "tableName");
        Scan scan = new Scan();
        try {
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String scanToString = Base64.encodeBytes(proto.toByteArray());
            configuration.set(TableInputFormat.SCAN, scanToString);

            //初始化Spark
            SparkConf conf = new SparkConf().setAppName("Test")
                    .setMaster("local[1]")
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .registerKryoClasses(new Class[]{});
            SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

            finalSchema(configuration, sparkSession);
            dynamicSchema(configuration, sparkSession);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过phoenix获取hbase数据
     */
    private static void finalSchema(Configuration configuration, SparkSession sparkSession) {
        JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
        JavaPairRDD<ImmutableBytesWritable, Result> javaPairRDD = context.newAPIHadoopRDD(configuration, TableInputFormat.class,
                ImmutableBytesWritable.class, Result.class);
        JavaRDD<Row> javaRDD = javaPairRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
            @Override
            public Row call(Tuple2<ImmutableBytesWritable, Result> tuple2) throws Exception {
                Result result = tuple2._2();
                String rowKey = Bytes.toString(result.getRow());
                String id = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("id")));
                String account = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("account")));
                String password = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("password")));
                return RowFactory.create(rowKey, id, account, password);
            }
        });
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("account", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("password", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> dataset = sparkSession.createDataFrame(javaRDD, schema);
    }

    /**
     * 动态配置schema的形式，比如说通过phoenix直接读取整个schema
     */
    private static void dynamicSchema(Configuration configuration, SparkSession sparkSession) {
        try {
            Connection connection = PhoenixUtil.getConnection();
            Statement statement = connection.createStatement();
            String sql = "select * from tableName limit 1";
            ResultSet resultSet = statement.executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            Map<String, String> columnNameMap = new LinkedHashMap<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                String columnTypeName = metaData.getColumnTypeName(i);
                String columnName = metaData.getColumnName(i);
                columnNameMap.put(columnName, columnTypeName);
            }
            PhoenixUtil.returnConnection(connection);

            JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
            JavaPairRDD<ImmutableBytesWritable, Result> javaPairRDD = context.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            JavaRDD<Row> map = javaPairRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
                @Override
                public Row call(Tuple2<ImmutableBytesWritable, Result> tuple2) throws Exception {
                    Result result = tuple2._2();
                    String row = Bytes.toString(result.getRow());
                    List<String> valueList = new ArrayList<>();
                    for (String column : columnNameMap.keySet()) {
                        String columnValue = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(column)));
                        valueList.add(columnValue);
                    }
                    String[] values = valueList.toArray(new String[]{});
                    return RowFactory.create(row, values);
                }
            });

            List<StructField> fieldList = new ArrayList<>();
            for (Map.Entry<String, String> entry : columnNameMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                DataType dataType = getDataType(value);
                fieldList.add(DataTypes.createStructField(key, dataType, false));
            }
            StructType schema = DataTypes.createStructType(fieldList);
            Dataset<Row> dataset = sparkSession.createDataFrame(map, schema);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static DataType getDataType(String typeName) {
        DataType dataType = null;
        if ("string".equals(typeName)) {
            dataType = DataTypes.StringType;
        } else if ("boolean".equals(typeName)) {
            dataType = DataTypes.BooleanType;
        } else if ("dobule".equals(typeName)) {
            dataType = DataTypes.DoubleType;
        } else if ("date".equals(typeName)) {
            dataType = DataTypes.DateType;
        } else if ("float".equals(typeName)) {
            dataType = DataTypes.FloatType;
        } else if ("bigint".equals(typeName)) {
            dataType = DataTypes.LongType;
        } else if ("short".equals(typeName)) {
            dataType = DataTypes.ShortType;
        } else if ("byte".equals(typeName)) {
            dataType = DataTypes.ByteType;
        } else if ("timestamp".equals(typeName)) {
            dataType = DataTypes.TimestampType;
        }
        return dataType;
    }
}
