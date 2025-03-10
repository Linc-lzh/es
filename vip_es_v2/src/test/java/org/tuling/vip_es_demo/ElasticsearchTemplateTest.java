package org.tuling.vip_es_demo;

import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.*;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.data.elasticsearch.core.query.highlight.Highlight;
import org.springframework.data.elasticsearch.core.query.highlight.HighlightField;
import org.springframework.data.elasticsearch.core.query.highlight.HighlightParameters;
import org.tuling.vip_es_demo.bean.Employee;

import java.util.*;

/**
 * @author: Fox
 * @Desc:
 **/

@Slf4j
public class ElasticsearchTemplateTest extends VipEsDemoApplicationTests{

    @Autowired
    ElasticsearchTemplate elasticsearchTemplate;

    @Test
    public void testIndex(){
        IndexOperations indexOps = elasticsearchTemplate.indexOps(IndexCoordinates.of("employee_demo"));
        boolean exists = indexOps.exists();
        if (!exists){
            log.info("索引不存在");
        }else {
            indexOps.delete();
        }
        //创建索引
        //设置setting
        /*
        "number_of_shards": "1",
        "number_of_replicas": "1",
         */
        Map<String, Object> settings = new HashMap<>();
        settings.put("number_of_shards","1");
        settings.put("number_of_replicas","1");
        //设置mapping
        Document mapping = Document.parse("{\n" +
                "      \"properties\": {\n" +
                "        \"_class\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"fields\": {\n" +
                "            \"keyword\": {\n" +
                "              \"type\": \"keyword\",\n" +
                "              \"ignore_above\": 256\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"address\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"fields\": {\n" +
                "            \"keyword\": {\n" +
                "              \"type\": \"keyword\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"analyzer\": \"ik_max_word\"\n" +
                "        },\n" +
                "        \"age\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"id\": {\n" +
                "          \"type\": \"long\"\n" +
                "        },\n" +
                "        \"name\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"remark\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"fields\": {\n" +
                "            \"keyword\": {\n" +
                "              \"type\": \"keyword\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"analyzer\": \"ik_smart\"\n" +
                "        },\n" +
                "        \"sex\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        }\n" +
                "      }\n" +
                "    }");
        indexOps.create(settings,mapping);


    }

    @Test
    public void testCreateIndex(){

        //索引是否存在
        boolean exist = elasticsearchTemplate.indexOps(Employee.class).exists();
        if(exist){
            //删除索引
            elasticsearchTemplate.indexOps(Employee.class).delete();
        }
        //创建索引
        //1）配置settings
        Map<String, Object> settings = new HashMap<>();
        //"number_of_shards": 1,
        //"number_of_replicas": 1
        settings.put("number_of_shards",1);
        settings.put("number_of_replicas",1);
        //2) 配置mapping
        String json = "{\n" +
                "      \"properties\": {\n" +
                "        \"_class\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"fields\": {\n" +
                "            \"keyword\": {\n" +
                "              \"type\": \"keyword\",\n" +
                "              \"ignore_above\": 256\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"address\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"fields\": {\n" +
                "            \"keyword\": {\n" +
                "              \"type\": \"keyword\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"analyzer\": \"ik_max_word\"\n" +
                "        },\n" +
                "        \"age\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"id\": {\n" +
                "          \"type\": \"long\"\n" +
                "        },\n" +
                "        \"name\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"remark\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"fields\": {\n" +
                "            \"keyword\": {\n" +
                "              \"type\": \"keyword\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"analyzer\": \"ik_smart\"\n" +
                "        },\n" +
                "        \"sex\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        }\n" +
                "      }\n" +
                "    }";
        Document mapping = Document.parse(json);
        //3)创建索引
        elasticsearchTemplate.indexOps(Employee.class)
                .create(settings,mapping);

        //查看索引mappings信息
        Map<String, Object> mappings = elasticsearchTemplate.indexOps(Employee.class).getMapping();
        log.info(mappings.toString());


    }


    @Test
    public void testBulkBatchInsert(){
        List<Employee> employees = new ArrayList<>();
        employees.add(new Employee(12L,"张三11",1,25,"广州天河公园","java developer"));
        employees.add(new Employee(13L,"李四11",1,28,"广州荔湾大厦","java assistant"));
        employees.add(new Employee(14L,"小红11",0,26,"广州白云山公园","php developer"));

        List<IndexQuery> bulkInsert = new ArrayList<>();
        for (Employee employee : employees) {
            IndexQuery indexQuery = new IndexQuery();
            //设置_id
            indexQuery.setId(String.valueOf(employee.getId()));
            String json = JSONObject.toJSONString(employee);
            //设置_source
            indexQuery.setSource(json);
            bulkInsert.add(indexQuery);
        }
        //bulk批量插入文档
        elasticsearchTemplate.bulkIndex(bulkInsert,Employee.class);
    }


    @Test
    public void testDocument(){

        //根据id删除文档
        //对应： DELETE /employee/_doc/12
        elasticsearchTemplate.delete(String.valueOf(12L),Employee.class);

        Employee employee = new Employee(12L,"张三三",1,25,"广州天河公园","java developer");
        //插入文档
        elasticsearchTemplate.save(employee);

        //根据id查询文档
        //对应：GET /employee/_doc/12
        Employee emp = elasticsearchTemplate.get(String.valueOf(12L),Employee.class);
        log.info(String.valueOf(emp));

    }

    @Test
    public void testQuery(){
        //条件查询
        /* 查询姓名为张三的员工信息
        GET /employee/_search
        {
            "query": {
            "term": {
                "name": {
                    "value": "张三"
                }
            }
        }
        }*/
        //1.构建查询语句
        //1.1 构建StringQuery
        //Query query = new StringQuery("{\"term\":{\"name\":{\"value\":\"张三\"}}}");
        //1.2 构建NativeQuery
        Query query = NativeQuery.builder()
                //.withQuery(QueryBuilders.term(t->t.field("name").value("张三")))
                .withQuery(w->w.term(t->t.field("name").value("张三")))
                .build();

        //2.通过search查询
        SearchHits<Employee> search = elasticsearchTemplate.search(query, Employee.class);
        //3.解析结果
        List<SearchHit<Employee>> searchHits = search.getSearchHits();
        for (SearchHit<Employee> searchHit : searchHits){
            log.info("search:"+ searchHit.toString());
        }


    }

    @Test
    public void testQueryDocument(){
        //条件查询
        /* 查询姓名为张三的员工信息
        GET /employee/_search
        {
            "query": {
            "term": {
                "name": {
                    "value": "张三"
                }
            }
        }
        }*/

        //第一步：构建查询语句
        //方式1：StringQuery
//        Query query = new StringQuery("{\n" +
//                "            \"term\": {\n" +
//                "                \"name\": {\n" +
//                "                    \"value\": \"张三\"\n" +
//                "                }\n" +
//                "            }\n" +
//                "        }");
        //方式2：NativeQuery
        Query query = NativeQuery.builder()
                .withQuery(q -> q.term(
                        t -> t.field("name").value("张三")))
                .build();


        //第二步：调用search查询
        SearchHits<Employee> search = elasticsearchTemplate.search(query, Employee.class);
        //第三步：解析返回结果
        List<SearchHit<Employee>> searchHits = search.getSearchHits();
        for (SearchHit hit: searchHits){
            log.info("返回结果："+hit.toString());
        }


    }


    @Test
    public void testMatchQueryDocument(){
        //条件查询
        /*最少匹配广州，公园两个词
        GET /employee/_search
        {
            "query": {
            "match": {
                "address": {
                    "query": "广州公园",
                     "minimum_should_match": 2
                }
            }
        }
        }*/

        //第一步：构建查询语句
        //方式1：StringQuery
//        Query query = new StringQuery("{\n" +
//                "            \"match\": {\n" +
//                "                \"address\": {\n" +
//                "                    \"query\": \"广州公园\",\n" +
//                "                     \"minimum_should_match\": 2\n" +
//                "                }\n" +
//                "            }\n" +
//                "        }");
        //方式2：NativeQuery
        Query query = NativeQuery.builder()
                .withQuery(q -> q.match(
                        m -> m.field("address").query("广州公园")
                                .minimumShouldMatch("2")))
                .build();


        //第二步：调用search查询
        SearchHits<Employee> search = elasticsearchTemplate.search(query, Employee.class);
        //第三步：解析返回结果
        List<SearchHit<Employee>> searchHits = search.getSearchHits();
        for (SearchHit hit: searchHits){
            log.info("返回结果："+hit.toString());
        }

    }

    @Test
    public void testQueryDocument3(){
        // 分页排序高亮
        /*
        GET /employee/_search
        {
          "from": 0,
          "size": 3,
          "query": {
            "match": {
              "remark": {
                "query": "JAVA"
              }
            }
          },
          "highlight": {
            "pre_tags": ["<font color='red'>"],
            "post_tags": ["<font/>"],
            "require_field_match": "false",
            "fields": {
              "*":{}
            }
          },
          "sort": [
            {
              "age": {
                "order": "desc"
              }
            }
          ]
        }*/
        //第一步：构建查询语句
        Query query = new StringQuery("{\n" +
                "        \"match\": {\n" +
                "          \"remark\": {\n" +
                "            \"query\": \"JAVA\"\n" +
                "          }\n" +
                "        }\n" +
                "      }");
        //分页  注意：from = pageNumber（页码，从0开始，） * pageSize（每页的记录数）
        query.setPageable(PageRequest.of(0, 3));
        //排序
        query.addSort(Sort.by(Order.desc("age")));
        //高亮
        HighlightField highlightField = new HighlightField("*");
        HighlightParameters highlightParameters = new HighlightParameters.HighlightParametersBuilder()
                .withPreTags("<font color='red'>")
                .withPostTags("<font/>")
                .withRequireFieldMatch(false)
                .build();
        Highlight highlight = new Highlight(highlightParameters,Arrays.asList(highlightField));
        HighlightQuery highlightQuery = new HighlightQuery(highlight,Employee.class);

        query.setHighlightQuery(highlightQuery);


        //第二步：调用search查询
        SearchHits<Employee> search = elasticsearchTemplate.search(query, Employee.class);
        //第三步：解析返回结果
        List<SearchHit<Employee>> searchHits = search.getSearchHits();
        for (SearchHit hit: searchHits){
            log.info("返回结果："+hit.toString());
        }
    }


    @Test
    public void testBoolQueryDocument(){
        //条件查询
        /*
        GET /employee/_search
        {
          "query": {
            "bool": {
              "must": [
                {
                  "match": {
                    "address": "广州"
                  }
                },{
                  "match": {
                    "remark": "java"
                  }
                }
              ]
            }
          }
        }
         */

        //第一步：构建查询语句
        //方式1：StringQuery
//        Query query = new StringQuery("{\n" +
//                "            \"bool\": {\n" +
//                "              \"must\": [\n" +
//                "                {\n" +
//                "                  \"match\": {\n" +
//                "                    \"address\": \"广州\"\n" +
//                "                  }\n" +
//                "                },{\n" +
//                "                  \"match\": {\n" +
//                "                    \"remark\": \"java\"\n" +
//                "                  }\n" +
//                "                }\n" +
//                "              ]\n" +
//                "            }\n" +
//                "          }");
        //方式2：NativeQuery
        Query query = NativeQuery.builder()
                .withQuery(q -> q.bool(
                        m -> m.must(
                          QueryBuilders.match( q1 -> q1.field("address").query("广州")),
                          QueryBuilders.match( q2 -> q2.field("remark").query("java"))
                        )))
                .build();


        //第二步：调用search查询
        SearchHits<Employee> search = elasticsearchTemplate.search(query, Employee.class);
        //第三步：解析返回结果
        List<SearchHit<Employee>> searchHits = search.getSearchHits();
        for (SearchHit hit: searchHits){
            log.info("返回结果："+hit.toString());
        }

    }

}
