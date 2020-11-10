package com.bigdata.es;


import com.alibaba.fastjson.JSONObject;
import io.netty.util.Mapping;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexResponse;
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
     * 设置元数据信息 方法
     * @param indexName     数据库
     * @param typeName      表名
     * @param mapping       元数据
     */
    public void setMapping(String indexName, String typeName, String mapping) {
        getAdminClient().preparePutMapping(indexName).setType(typeName)
                .setSource(mapping, XContentType.JSON).get();
    }


    /**
     * 设置元数据JSON格式
     */
    public static void setMappingTest(){
//        这是三层嵌套的json格式
       JSONObject mapping = new JSONObject();
       JSONObject properties = new JSONObject();
//      设置每个字段
       JSONObject idJSON = new JSONObject();
       idJSON.put("type", "keyword");
       idJSON.put("store", "true");
       properties.put("id", idJSON);

       JSONObject nameJSON = new JSONObject();
       nameJSON.put("type", "keyword");
       properties.put("name", nameJSON);

       JSONObject uidJSON = new JSONObject();
       uidJSON.put("type", "keyword");
//       store：是否单独设置此字段的存储是否从_source字段中分离，只能搜索，不能获取值
       uidJSON.put("store", "false");
       properties.put("name", uidJSON);

       JSONObject hotelJSON = new JSONObject();
       hotelJSON.put("type", "text");
       properties.put("name", hotelJSON);

       JSONObject happendDate = new JSONObject();
       happendDate.put("type", "date");
       happendDate.put("format", "yyyy-MM-dd");
       properties.put("name", happendDate);

       mapping.put("properties",properties);
       EsCurdDemo esCurdDemo = new EsCurdDemo();
       esCurdDemo.setMapping("test_index1","test1",mapping.toString());

   }


    /**
     *
     * @param index     索引（数据库）
     * @param type      类型（表名）
     * @param id        id
     * @param source
     * @return
     */
   public long addDoc(String index,String type,String id,Map<String,Object> source){
       IndexResponse indexResponse = getClient().prepareIndex(index, type, id).setSource(source).get();
       return indexResponse.getVersion();
   }



    /**
     * 查询文档
     * @param text
     * @return
     */
    public List<Map<String,Object>> queryStringQuery(String text){
        QueryStringQueryBuilder match = QueryBuilders.queryStringQuery(text);
//        这里可以指定索引名称，不指定默认是所有
        SearchRequestBuilder search = getClient().prepareSearch("movie_index").setQuery(match);
        SearchResponse response = search.get();
//        命中的文档
        SearchHits hits = response.getHits();
//        命中文档的数量
        long totalHits = hits.getTotalHits();
//        命中文档的内容
        SearchHit[] searchHits = hits.getHits();
        ArrayList<Map<String,Object>> maps = new ArrayList();
        for (SearchHit hit : searchHits) {
//            文档索引
            String index = hit.getIndex();
//            文档的数据
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

//        esCurdDemo.setMapping("test_index1","test1",XContentType.JSON);

//        setMappingTest();

//

//        List<Map<String, Object>> queryText = esCurdDemo.queryStringQuery("river");
//        System.out.println(queryText);
//      [{doubanScore=8.0, name=operation meigong river, actorList=[{name=zhang han yu, id=3}], id=2}]

//            esCurdDemo.addDoc("test_index2","test2",3,"[{doubanScore=8.0, name=operation meigong river, actorList=[{name=zhang han yu, id=3}], id=2}]");

    }
}
