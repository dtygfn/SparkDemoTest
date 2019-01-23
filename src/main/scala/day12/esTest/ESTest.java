package day12.esTest;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;

public class ESTest{
    // es的连接
    private Client client;

    @Before
    public void getClient() throws UnknownHostException {
        // 配置请求es的信息
        // 注意如果集群的名字如果是elasticsearch，就不用该配置了
        final HashMap<String,String> map = new HashMap<>();
        map.put("cluster.name","es");
        final Settings.Builder settins = Settings.builder().put(map);
        // es的java api提供的端口为9300
        // 可以再添加多个节点，目的就是为了其中一个实例出现网络的时候切换到别的实例
        client = TransportClient.builder().settings(settins).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("mini4"),9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("mini5"),9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("mini6"),9300));

    }

    /**
     * 使用json来创建文档并写入数据
     */
    @Test
    public void createDoc_1(){
        // json字符串
        String source = "{" +
                "\"id\":\"1\"," +
                "\"title\":\"ElasticSearch是一个基于Lucene的搜索服务器\"," +
                "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"}";
        // 创建文档，定义索引，文档类型，主键id
        final IndexResponse indexResponse = client.prepareIndex("blog","article","1").setSource(source).get();// 也可以用 execute().actionGet()

        // 获取响应的信息
        System.out.println("index:"+indexResponse.getIndex());
        System.out.println("type:"+indexResponse.getType());
        System.out.println("id:"+indexResponse.getId());
        System.out.println("version:"+indexResponse.getVersion());
        System.out.println("是否创建成功：" + indexResponse.isCreated());

        client.close();
    }

    /**
     * 使用map插入数据
     */
    @Test
    public void createDoc_2(){
        final HashMap<String,Object> source = new HashMap<>();
        source.put("id",2);
        source.put("title", "ElasticSearch");
        source.put("content", "我们建立一个网站或应用程序，并要添加搜索功能");

        final IndexResponse indexResponse = client.prepareIndex("blog","article","2").setSource(source).get();

        // 获取响应的信息
        System.out.println("index:"+indexResponse.getIndex());
        System.out.println("type:"+indexResponse.getType());
        System.out.println("id:"+indexResponse.getId());
        System.out.println("version:"+indexResponse.getVersion());
        System.out.println("是否创建成功：" + indexResponse.isCreated());

        client.close();
    }

    /**
     * 使用es的帮助类插入数据
     *
     */
    @Test
    public void createDoc_3() throws Exception {
       final XContentBuilder source = XContentFactory.jsonBuilder()
               .startObject()
               .field("id",3)
               .field("title","Lucene的搜索服务器")
               .field("content","并作为Apache许可条款下的开放源码发布")
               .endObject();
       client.prepareIndex("blog","article","3").setSource(source).get();

       client.close();
    }

    /**
     * 搜索文档，单个索引
     */
    @Test
    public void getData_1(){
        final GetResponse getResponse = client.prepareGet("blog","article","1").get();
        System.out.println(getResponse.getSourceAsString());

        client.close();

    }
    /**
     * 搜索文档，多个索引
     */
    @Test
    public void getData_2(){
        final MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("blog", "article", "1")
                .add("blog", "article", "2")
                .add("blog", "article", "100")
                .get();

        // 遍历
        for (MultiGetItemResponse multiGetItemRespon : multiGetItemResponses) {
            final GetResponse response = multiGetItemRespon.getResponse();
            // 判断数据是否存在
            if (response.isExists()){
                System.out.println(response.getSourceAsString());
            }

        }

        client.close();
    }

    /**
     * 更新数据
     */
    @Test
    public void updateData_1() throws Exception{

        final UpdateRequest request = new UpdateRequest();
        request.index("blog");
        request.type("article");
        request.id("1");
        request.doc(XContentFactory.jsonBuilder()
                .startObject()
                .field("id","1")
                .field("title", "更新：ElasticSearch是一个基于Lucene的搜索服务器")
                .field("content", "更新：它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口")
                .endObject());
        client.update(request).get();
        client.close();
    }

    /**
     * 更新数据
     */
    @Test
    public void updateData_2() throws Exception{
        client.update(new UpdateRequest("blog","article","2")
                        .doc((XContentFactory.jsonBuilder()
                                .startObject()
                                .field("id", "2")
                                .field("title", "更新：ElasticSearch")
                                .field("content", "更新：我们建立一个网站或应用程序，并要添加搜索功能，基于RESTful web接口")
                                .endObject()))).get();

        client.close();


    }
    /**
     * 更新文档数据，设置一个查询条件，查询id，如果查不到数据就添加，如果查到就更新
     */
    @Test
    public void updateData_3() throws Exception{
        final IndexRequest source = new IndexRequest("blog", "article", "4")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", "4")
                        .field("title", "我们希望能够简单地使用JSON")
                        .field("content", "我们希望我们的搜索服务器始终可用")
                        .endObject());
        // 设置更新的数据
        final UpdateRequest upsert = new UpdateRequest("blog", "article", "4")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("title", "ElasticSearch是一个基于Lucene的搜索服务器")
                        .endObject())
                .upsert(source);

        client.update(upsert).get();

        client.close();
    }

    /**
     * 删除文档数据
     */
    @Test
    public void deleteData(){
        client.prepareDelete("blog","article","3").get();
        client.close();
    }

    /**
     * 检索：SearchResponse，支持各种查询
     */
    @Test
    public void search(){
        // 在没有用到中文分词器的情况下（es也自带了分词器），是不能按照词来进行搜索的
        // queryStringQuery方法，是按照单个字来搜索的
        final SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("搜索"))  // 其实是按照单个字进行检索
                .get();

        // 获取数据的结果集对象，可以获取命中次数
        final SearchHits hits = searchResponse.getHits();
        System.out.println("查询的数据有"+hits.getTotalHits()+"条");

        // 遍历每一条数据
        final Iterator<SearchHit> it = hits.iterator();
        while (it.hasNext()){
            final SearchHit searchHit = it.next();
            System.out.println("整条数据为:"+searchHit.getSourceAsString());
            System.out.println("id:"+searchHit.getId()); // 获取的id为系统生成的  _id
            System.out.println("id:"+searchHit.getSource().get("id"));
            System.out.println("title:"+searchHit.getSource().get("title"));
            System.out.println("content:" + searchHit.getSource().get("content"));
        }

        client.close();

    }

    /**
     * 创建索引(ik分词器之后）
     */
    @Test
    public void createIndex(){
        // 创建索引
        client.admin().indices().prepareCreate("blog").get();
        // 删除索引
//        client.admin().indices().prepareDelete("blog").get();

    }
    /**
     * 创建映射，指定分词器
     */
    @Test
    public void createIndexMapping() throws Exception{
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("article")
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "integer").field("store", "yes")
                            .endObject()
                            .startObject("title")
                                .field("type", "string").field("store", "yes").field("analyzer", "ik")
                            .endObject()
                            .startObject("content")
                                .field("type", "string").field("store", "yes").field("analyzer", "ik")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        final PutMappingRequest source = Requests.putMappingRequest("blog")
                .type("article")
                .source(mappingBuilder);

        client.admin().indices().putMapping(source).get();

        client.close();

    }
    /**
     * 查询所有数据
     */
    @Test
    public void queryAll(){
        final SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
        getResponse(searchResponse);
        client.close();
    }
    /**
     * 解析查询字符串
     */
    @Test
    public void queryString(){
        final SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("搜索").field("content").field("title"))
                .get();
        getResponse(searchResponse);
        client.close();


    }
    /**
     * 通配符查询
     *
     */
    @Test
    public void wildCardQuery(){
        final SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.wildcardQuery("content", "*搜"))
                .get();

        getResponse(searchResponse);
        client.close();
    }

    /**
     * 词条查询
     *
     */
    @Test
    public void termQuery(){
        final SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
//                .setQuery(QueryBuilders.termQuery("title", "搜索"))
                .setQuery(QueryBuilders.termsQuery("title", "搜索", "千峰"))// 搜索多个词条，有一个存在则符合
                .get();

        getResponse(searchResponse);
        client.close();
    }

    /**
     * 字段查询
     */
    @Test
    public void fieldMatchQuery(){
        final SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                //该方法需要给一个匹配度的值，这样可以给一个不完整的词的查询
                // 后面的值是相似度
//                .setQuery(QueryBuilders.matchQuery("title", "搜索").analyzer("ik").fuzziness(1))
                //给某个词的前缀也可以查询
//                .setQuery(QueryBuilders.matchPhraseQuery("title","服"))
                .setQuery(QueryBuilders.multiMatchQuery("搜索","title","content"))
                .get();

        getResponse(searchResponse);
        client.close();

    }

    /**
     * id 查询
     *
     */
     @Test
     public void idQuery(){
         final SearchResponse searchResponse = client.prepareSearch("blog")
                 .setTypes("article")
                 .setQuery(QueryBuilders.idsQuery().ids("2", "4"))
                 .get();
         getResponse(searchResponse);
         client.close();
     }

    /**
     * 相似度查询
     */
    @Test
    public void fuzzyQuery(){
        final SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.fuzzyQuery("title", "Lueene"))
                .get();
        getResponse(searchResponse);
        client.close();

    }

    /**
     * 范围查询
     *
     */
    @Test
    public void rangeQuery(){
        final SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                //查询id大于1小于3 的数据
//                .setQuery(QueryBuilders.rangeQuery("id").gte(1).lte(3))
                .setQuery(QueryBuilders.rangeQuery("id").from(1).to(3))
                .get();
        getResponse(searchResponse);
        client.close();

    }

    /**
     * bool查询，需要和其他查询结合在一起
     */
    @Test
    public void boolQuery(){
        final SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(
                        QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("title", "搜索"))
                        .should(QueryBuilders.rangeQuery("id").from(1).to(3))
                ).get();

        getResponse(searchResponse);
        client.close();
    }

    /**
     * 排序查询
     */
    @Test
    public void sortQuery(){
        final SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termsQuery("title", "搜索"))
                        .must(QueryBuilders.rangeQuery("id").from(1).to(2)))
                .addSort("id", SortOrder.DESC)
                .get();

        getResponse(searchResponse);
        client.close();
    }

    public static void getResponse(SearchResponse searchResponse){
        // 获取数据的结果集对象，可以获取命中次数
        final SearchHits hits = searchResponse.getHits();
        System.out.println("查询的数据有"+hits.getTotalHits()+"条");

        // 遍历每一条数据
        final Iterator<SearchHit> it = hits.iterator();
        while (it.hasNext()){
            final SearchHit searchHit = it.next();
            System.out.println("整条数据为:"+searchHit.getSourceAsString());
            System.out.println("id:"+searchHit.getId()); // 获取的id为系统生成的  _id
            System.out.println("id:"+searchHit.getSource().get("id"));
            System.out.println("title:"+searchHit.getSource().get("title"));
            System.out.println("content:" + searchHit.getSource().get("content"));
        }



    }
}
