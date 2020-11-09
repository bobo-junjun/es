package com.bigdata.es;


import io.netty.util.Mapping;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 获取es的客户端 client
 */
public class EsCurdDemo {
    private Settings settings = Settings.builder().put("cluster.name", "my-es")
            .put("client.transport.sniff", false).build();
    private TransportClient client;

    /**
     * 获取es的客户端 client
     * @return
     */
    public TransportClient getClient() {
        if (client == null) {
            synchronized (TransportClient.class) {
                try {
                 client =   new PreBuiltTransportClient(settings).addTransportAddresses(
                            new TransportAddress(InetAddress.getByName("yb05"), 9300)
                            ,new TransportAddress(InetAddress.getByName("yb06"), 9300),
                            new TransportAddress(InetAddress.getByName("yb07"), 9300)
                    );
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
            }
        }
        return client;
    }

    /**
     * 获取索引管理的IndicesAdminClient
     *
     * @return
     */
    public IndicesAdminClient getAdminClient() {
        return getClient().admin().indices();
    }

    /**
     * 判断下标是否存在
     *
     * @param indexName
     * @return
     */
    public boolean isExistsIndex(String indexName) {
        IndicesExistsResponse response = getAdminClient().prepareExists(indexName).get();
        return response.isExists();
    }

    /**
     * 创建索引
     *
     * @param indexName
     * @return
     */
    public boolean createIndex(String indexName) {
        CreateIndexResponse response = getAdminClient().prepareCreate(indexName.toLowerCase()).get();
        return response.isAcknowledged();
    }

    /**
     * 删除索引
     *
     * @param indexName
     * @return
     */
    public boolean deleteIndex(String indexName) {
        AcknowledgedResponse response = getAdminClient().prepareDelete(indexName).execute().actionGet();
        return response.isAcknowledged();
    }

    /**
     * 设置元数据信息
     * @param indexName     数据库
     * @param typeName      表名
     * @param mapping       元数据
     */
    public void setMapping(String indexName, String typeName, XContentType mapping) {
        getAdminClient().preparePutMapping(indexName).setType(typeName)
                .setSource(mapping, XContentType.JSON).get();
    }

    public List<Map<String,Object>> queryStringQuery(String text){
        QueryStringQueryBuilder match = QueryBuilders.queryStringQuery(text);
        SearchRequestBuilder search = getClient().prepareSearch().setQuery(match);
        SearchResponse response = search.get();
//        命中的文档
        SearchHits hits = response.getHits();
//        命中文档的数量
        long totalHits = hits.getTotalHits();
        SearchHit[] searchHits = hits.getHits();
        ArrayList<Map<String,Object>> maps = new ArrayList();
        for (SearchHit hit : searchHits) {
//            文档元数据
            String index = hit.getIndex();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            maps.add(sourceAsMap);
        }
        return maps;
    }


    public static void main(String[] args) {
        EsCurdDemo esCurdDemo = new EsCurdDemo();
//        System.out.println(esCurdDemo.isExistsIndex("movie_index"));
//        System.out.println(esCurdDemo.isExistsIndex("test_index1"));
//        System.out.println(esCurdDemo.createIndex("test_index2"));
//        System.out.println(esCurdDemo.isExistsIndex("test_index2"));
//        System.out.println(esCurdDemo.deleteIndex("test_index2"));
//        System.out.println(esCurdDemo.isExistsIndex("test_index2"));
//        System.out.println(esCurdDemo.createIndex("test_index1"));

        esCurdDemo.setMapping("test_index1","test1",XContentType.JSON);



    }
}
