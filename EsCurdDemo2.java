package com.bigdata.es;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
public class EsCurdDemo2 {
    private Settings settings = Settings.builder().put("cluster.name", "my-es").
            put("client.transport.sniff", false).build();
    private TransportClient client;

    /**
     * 获取es的客户端 client
     *
     * @return
     * @throws UnknownHostException
     */
    public TransportClient getClient() {
        if (client == null) {
            synchronized (TransportClient.class) {
                try {
                    client = new PreBuiltTransportClient(settings).
                            addTransportAddress(
                                    new TransportAddress(InetAddress.getByName("yb05"), 9300));
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

    public boolean deleteIndex(String indexName) {
        AcknowledgedResponse response = getAdminClient().prepareDelete(indexName).execute().actionGet();
        return response.isAcknowledged();
    }

    public void setMapping(String indexName, String typeName, String mapping) {
        getAdminClient().preparePutMapping(indexName).setType(typeName).setSource(mapping, XContentType.JSON).get();
    }

    public static void main(String[] args) {
        EsCurdDemo2 demo2 = new EsCurdDemo2();
        System.out.println(demo2.isExistsIndex("movie_index"));

    }
}
