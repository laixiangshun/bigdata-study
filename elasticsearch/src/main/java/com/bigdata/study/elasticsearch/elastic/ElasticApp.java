package com.bigdata.study.elasticsearch.elastic;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * elasticsearch 实操
 * TransportClient 采用tcp协议
 **/
public class ElasticApp {

    public static void main(String[] args) {
        Settings settings = Settings.builder()
                .put("cluster.name", "es-cluster").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.20.48"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

    }

    private static void esInfo(TransportClient client) {
        List<DiscoveryNode> discoveryNodes = client.connectedNodes();
        for (DiscoveryNode node : discoveryNodes) {
            String builder = String.valueOf(node.getAddress()) + "\n" +
                    node.getHostName() + "\n" +
                    node.getHostAddress() + "\n" +
                    node.getVersion() + "\n" +
                    node.getName();
            System.out.println(builder);
        }
    }

    /**
     * 准备数据
     */
    private static void prepareData(TransportClient client) throws IOException {
        IndexResponse indexResponse = client.prepareIndex("company", "employee", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "jack")
                        .field("age", 27)
                        .field("position", "technique software")
                        .field("country", "china")
                        .field("join_date", "2017-01-01")
                        .field("salary", 10000).endObject())
                .get();
        if (!indexResponse.isCreated()) {
            throw new RuntimeException("create index error");
        }
        client.prepareIndex("company", "employee", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "marry")
                        .field("age", 33)
                        .field("position", "technique software")
                        .field("country", "china")
                        .field("join_date", "2017-01-02")
                        .field("salary", 12000).endObject())
                .get();
        client.prepareIndex("company", "employee", "3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "tom")
                        .field("age", 32)
                        .field("position", "senior technique software")
                        .field("country", "china")
                        .field("join_date", "2016-01-03")
                        .field("salary", 11000)
                        .endObject())
                .get();

        client.prepareIndex("company", "employee", "4")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "jen")
                        .field("age", 25)
                        .field("position", "junior finance")
                        .field("country", "usa")
                        .field("join_date", "2016-01-04")
                        .field("salary", 7000)
                        .endObject())
                .get();

        client.prepareIndex("company", "employee", "5")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "mike")
                        .field("age", 37)
                        .field("position", "finance manager")
                        .field("country", "usa")
                        .field("join_date", "2015-01-05")
                        .field("salary", 15000)
                        .endObject())
                .get();

    }

    /**
     * 简单查询
     */
    private static void executeSearch(TransportClient client) {
        SearchResponse searchResponse = client.prepareSearch("company")
                .setTypes("employee")
                .setQuery(QueryBuilders.matchQuery("position", "technique"))
                .setPostFilter(QueryBuilders.rangeQuery("age").from(20).to(40))
                .setFrom(0)
                .setSize(10)
                .get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 查询单条索引的全部信息
     */
    private static void search(TransportClient client) throws IOException {
        IndexResponse indexResponse = client.prepareIndex("company", "employee", "6")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "mike")
                        .field("age", 37)
                        .field("position", "finance manager")
                        .field("country", "usa")
                        .field("join_date", "2015-01-05")
                        .field("salary", 15000)
                        .endObject()
                ).get();
        String builder = indexResponse.getIndex() + "\n" +
                indexResponse.getType() + "\n" +
                indexResponse.getVersion() + "\n" +
                indexResponse.getId() + "\n" +
                indexResponse.getShardInfo();
        System.out.println(builder);
    }

    /**
     * 查询单条数据
     */
    private static void getSearch(TransportClient client) {
        GetResponse response = client.prepareGet("company", "employee", "id").get();
        StringBuilder builder = new StringBuilder();
        builder.append(response.getIndex()).append("\n")
                .append(response.getType()).append("\n")
                .append(response.getVersion()).append("\n")
                .append(response.getId()).append("\n")
                .append(response.getSourceAsString());
        System.out.println(builder.toString());
        Map<String, Object> sourceAsMap = response.getSourceAsMap();
        for (Map.Entry<String, Object> entry : sourceAsMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

    /**
     * 查询多条数据
     *
     * @param client
     */
    private static void multiGet(TransportClient client) {
        //方法一
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("company", "", "1")
                .add("company", "employee", "2")
                .add("company", "employee", "3")
                .add("company", "employee", "4")
                .get();
        multiGetItemResponses.forEach(action -> {
            StringBuilder builder = new StringBuilder();
            builder.append(action.getIndex()).append("\n")
                    .append(action.getType()).append("\n")
                    .append(action.getId()).append("\n")
                    .append(action.getResponse().getSourceAsString());
            System.out.println(builder.toString());
        });

        //方法二
        MultiGetRequest request = new MultiGetRequest();
        request.add("company", "", "1");
        request.add("company", "employee", "2");
        request.add("company", "employee", "3");
        request.add("company", "employee", "4");
        ActionFuture<MultiGetResponse> multiGet = client.multiGet(request);
        MultiGetResponse response = multiGet.actionGet();
        response.forEach(action -> {
            StringBuilder builder = new StringBuilder();
            builder.append(action.getIndex()).append("\n")
                    .append(action.getType()).append("\n")
                    .append(action.getId()).append("\n")
                    .append(action.getResponse().getSourceAsString());
            System.out.println(builder.toString());
        });
    }

    /**
     * 使用bulk操作
     */
    public static void bulk(TransportClient client) throws IOException {
        BulkRequestBuilder prepareBulk = client.prepareBulk();
        prepareBulk.add(client.prepareIndex("company", "employee", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "jack")
                        .field("age", 27)
                        .field("position", "technique software")
                        .field("country", "china")
                        .field("join_date", "2017-01-01")
                        .field("salary", 10000).endObject()));
        prepareBulk.add(client.prepareIndex("company", "employee", "2")
                .setSource(XContentFactory.jsonBuilder().startObject().field("name", "marry")
                        .field("age", 33)
                        .field("position", "technique software")
                        .field("country", "china")
                        .field("join_date", "2017-01-02")
                        .field("salary", 12000).endObject()));
        BulkResponse bulkResponse = prepareBulk.get();
        StringBuilder builder = new StringBuilder();
        bulkResponse.forEach(action -> {
            ActionWriteResponse response = action.getResponse();
            builder.append(response.getShardInfo().toString()).append("\n");
        });
        System.out.println(builder.toString());
    }

    /**
     * 使用bulkprocessor 操作
     */
    public static void bulkProcessor(TransportClient client) throws InterruptedException {
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                System.out.println("---------before--------");
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                Arrays.stream(bulkResponse.getItems()).forEach(action -> System.out.println(action.getResponse()));
                System.out.println("-------after------------");
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                System.err.println(throwable.getMessage());
            }
        }).setBulkSize(new ByteSizeValue(50, ByteSizeUnit.MB))
                .setBulkActions(1000)
                .setConcurrentRequests(1)
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        bulkProcessor.add(new IndexRequest("company", "employee", "1"));
        bulkProcessor.add(new IndexRequest("company", "employee", "2"));
        bulkProcessor.flush();
        bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
        bulkProcessor.close();
        client.admin().indices().prepareFlush().get();
        SearchResponse searchResponse = client.prepareSearch().get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        StringBuilder builder = new StringBuilder();
        for (SearchHit hit : hits) {
            builder.append(hit.getSourceAsString()).append("\n");
            System.out.println(builder.toString());
        }
//        executeSearch(client);
    }

    /**
     * 聚合分析
     * 聚合提供了分组并统计数据的能力
     */
    private static void agg(TransportClient client) throws ExecutionException, InterruptedException {
        SearchResponse searchResponse = client.prepareSearch("company")
                .addAggregation(AggregationBuilders.terms("group_by_country").field("country")
                        .subAggregation(AggregationBuilders.dateHistogram("group_by_join_date")
                                .field("join_date")
                                .interval(DateHistogramInterval.YEAR))
                        .subAggregation(AggregationBuilders.avg("avg_salary").field("salary"))).execute().get();
        Map<String, Aggregation> aggregationMap = searchResponse.getAggregations().asMap();
        StringTerms stringTerms = (StringTerms) aggregationMap.get("group_by_country");
        stringTerms.getBuckets().forEach(bucket -> {
            System.out.println(bucket.getKey() + ":" + bucket.getDocCount());
            Histogram histogram = (Histogram) bucket.getAggregations().asMap().get("group_by_join_date");
            histogram.getBuckets().forEach(buc -> {
                System.out.println(buc.getKey() + ":" + buc.getDocCount());
                Avg avg = (Avg) buc.getAggregations().asMap().get("avg_salary");
                System.out.println(avg.getValue());
            });
        });
    }
}
