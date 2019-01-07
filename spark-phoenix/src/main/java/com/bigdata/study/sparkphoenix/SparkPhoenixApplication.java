package com.bigdata.study.sparkphoenix;

import javafx.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimePrinter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * spark通过Phoenix读取Hbase数据
 */
@SpringBootApplication
public class SparkPhoenixApplication {

    public static void main(String[] args) {
        SpringApplication.run(SparkPhoenixApplication.class, args);

        //初始化Spark
        SparkConf conf = new SparkConf().setAppName("Test")
                .setMaster("local[1]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{});
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        DateTime start = new DateTime(args[0]);
        DateTime end = new DateTime(args[1]);
        String startStr = start.toString("yyyy-MM-dd");
        String endStr = end.toString("yyyy-MM-dd");
        final String SQL_QUERY = "(SELECT date,member_id FROM events WHERE time>='%s' AND time<'%s' AND event='login') events";
        String sql = String.format(SQL_QUERY, startStr, endStr);

        //jdbc从Hbase读取数据
        Properties prop = new Properties();
        prop.put("driver", "org.apache.phoenix.jdbc.PhoenixDriver");
        prop.put("user", "");
        prop.put("password", "");
        prop.put("fetchsize", "10000");
        JavaRDD<Row> javaRDD = sparkSession.read()
                .jdbc("jdbc:phoenix:hadoop101,hadoop102,hadoop103", sql, prop)
                .filter("member_id!=-1")
                .javaRDD();
        JavaRDD<Row> rowJavaRDD = javaRDD.mapToPair(r -> new Tuple2<>(r.getString(0), r.getLong(1)))
                .distinct()
                .groupByKey()
                .map(r -> {
                    StringBuilder buffer = new StringBuilder();
                    r._2.forEach(buffer::append);
                    return RowFactory.create(r._1, buffer.toString());
                });

        //schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("date", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("dist_mem", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(fields);

        //去重并存储
        sparkSession.createDataFrame(rowJavaRDD, structType)
                .write()
                .format("org.apache.phoenix.spark")
                .mode(SaveMode.Overwrite)
                .option("table", "test_string")
                .option("zkUrl", "jdbc:phoenix:hadoop101,hadoop102,hadoop103")
                .save();
        sparkSession.stop();
        sparkSession.close();

    }

}

