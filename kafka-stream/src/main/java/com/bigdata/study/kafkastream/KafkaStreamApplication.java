package com.bigdata.study.kafkastream;

import com.bigdata.study.kafkastream.model.Item;
import com.bigdata.study.kafkastream.model.Order;
import com.bigdata.study.kafkastream.model.User;
import com.bigdata.study.kafkastream.serdes.SerdesFactory;
import com.bigdata.study.kafkastream.timeextractor.OrderTimestampExtractor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.Properties;

//@SpringBootApplication
public class KafkaStreamApplication {

    public static void main(String[] args) {
//        SpringApplication.run(KafkaStreamApplication.class, args);

        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-count");
        prop.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "192.168.20.48:2181,192.168.20.51:2181,192.168.20.52:2181");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.20.48:9092");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimestampExtractor.class);

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, Order> orderStream = builder.stream(Serdes.String(), SerdesFactory.serdeFrom(Order.class), "orders");
        KTable<String, User> userTable = builder.table(Serdes.String(), SerdesFactory.serdeFrom(User.class), "users", "users-state-store");
        KTable<String, Item> itemTable = builder.table(Serdes.String(), SerdesFactory.serdeFrom(Item.class), "items", "items-state-store");

        //订单与用户join，获取用户所在地
        KStream<String, OrderUser> orderUserStream = orderStream.leftJoin(userTable, OrderUser::fromOrderUser, Serdes.String(), SerdesFactory.serdeFrom(Order.class))
                .filter((String userName, OrderUser orderUser) -> StringUtils.isNotBlank(orderUser.getAddress()))
                .map((String userName, OrderUser orderUser) -> new KeyValue<>(orderUser.getItemName(), orderUser));

        //用户订单通过through 进行重新分区，满足于商品join条件：Topic的Key为产品名，并且二者的Partition数一样
        KStream<String, OrderUser> orderUserThrough = orderUserStream.through(Serdes.String(), SerdesFactory.serdeFrom(OrderUser.class), (String key, OrderUser orderUser, int num) ->
                (orderUser.getItemName().hashCode() & 0x7FFFFFFF) % num, "orderuser-repartition-by-item");

        //用户订单和商品join，获取商品所在地
        KStream<String, OrderUserItem> orderUserItemKStream = orderUserThrough.leftJoin(itemTable, OrderUserItem::fromOrderUserItem, Serdes.String(), SerdesFactory.serdeFrom(OrderUser.class))
                .filter((String itemName, OrderUserItem orderUserItem) -> StringUtils.compare(orderUserItem.getOrderUser().getAddress(), orderUserItem.getAddress()) == 0);

        //按性别统计用户订单价格总数
        KTable<String, Double> numTable = orderUserItemKStream.map((String itemName, OrderUserItem orderUserItem) -> KeyValue.pair(orderUserItem.getOrderUser().getGender(),
                orderUserItem.getPrice() * orderUserItem.getOrderUser().getQuantity()))
                .groupByKey(Serdes.String(), Serdes.Double())
                .reduce((Double v1, Double v2) -> v1 + v2, "gender-amount-state-store");
        numTable.toStream().map((String key, Double value) -> new KeyValue<>(key, String.valueOf(value))).to("gender-amount");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, prop);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        try {
            System.in.read();
            kafkaStreams.close();
            kafkaStreams.cleanUp();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static class OrderUser {
        private String userName;
        private String itemName;
        private long transactionDate;
        private int quantity;
        private String address;
        private String gender;
        private int age;

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getItemName() {
            return itemName;
        }

        public void setItemName(String itemName) {
            this.itemName = itemName;
        }

        public long getTransactionDate() {
            return transactionDate;
        }

        public void setTransactionDate(long transactionDate) {
            this.transactionDate = transactionDate;
        }

        public int getQuantity() {
            return quantity;
        }

        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getGender() {
            return gender;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public static OrderUser fromOrder(Order order) {
            OrderUser orderUser = new OrderUser();
            if (order == null) {
                return orderUser;
            }
            orderUser.setUserName(order.getUserName());
            orderUser.setItemName(order.getItemName());
            orderUser.setQuantity(order.getQuantity());
            orderUser.setTransactionDate(order.getTransactionDate());
            return orderUser;
        }

        public static OrderUser fromOrderUser(Order order, User user) {
            OrderUser orderUser = fromOrder(order);
            if (user == null) {
                return orderUser;
            }
            orderUser.setAddress(user.getAddress());
            orderUser.setAge(user.getAge());
            orderUser.setGender(user.getGender());
            return orderUser;
        }
    }

    private static class OrderUserItem {
        private String itemName;
        private String address;
        private String type;
        private double price;

        private OrderUser orderUser;

        public String getItemName() {
            return itemName;
        }

        public void setItemName(String itemName) {
            this.itemName = itemName;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        public OrderUser getOrderUser() {
            return orderUser;
        }

        public void setOrderUser(OrderUser orderUser) {
            this.orderUser = orderUser;
        }

        public static OrderUserItem fromOrderUser(OrderUser orderUser) {
            OrderUserItem orderUserItem = new OrderUserItem();
            if (orderUser == null) {
                return orderUserItem;
            }
            orderUserItem.setOrderUser(orderUser);
            return orderUserItem;
        }

        public static OrderUserItem fromOrderUserItem(OrderUser orderUser, Item item) {
            OrderUserItem orderUserItem = fromOrderUser(orderUser);
            if (item == null) {
                return orderUserItem;
            }
            orderUserItem.setAddress(item.getAddress());
            orderUserItem.setItemName(item.getItemName());
            orderUserItem.setPrice(item.getPrice());
            orderUserItem.setType(item.getType());
            return orderUserItem;
        }
    }
}

