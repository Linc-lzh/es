package org.tuling.vip_es_demo;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.Query;
import org.tuling.vip_es_demo.bean.Employee;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: Fox
 * @Desc:
 **/
@Slf4j
public class ElasticsearchClientTest extends VipEsDemoApplicationTests{

    @Autowired
    ElasticsearchClient elasticsearchClient;

    String indexName = "employee_demo";

    @Test
    public void testCreateIndex() throws IOException {

        //索引是否存在
        BooleanResponse exist = elasticsearchClient.indices()
                .exists(e->e.index(indexName));
        if(exist.value()){
            //删除索引
            elasticsearchClient.indices().delete(d->d.index(indexName));
        }
        //创建索引
        elasticsearchClient.indices().create(c->c.index(indexName)
                .settings(s->s.numberOfShards("1").numberOfReplicas("1"))
                .mappings(m-> m.properties("name",p->p.keyword(k->k))
                        .properties("sex",p->p.long_(l->l))
                        .properties("age",p->p.long_(l->l))
                        .properties("address",p->p.text(t->t.analyzer("ik_max_word")))
                )
        );

        //查询索引
        GetIndexResponse getIndexResponse = elasticsearchClient.indices().get(g -> g.index(indexName));
        log.info(getIndexResponse.result().toString());

    }


    @Test
    public void testBulkBatchInsert() throws IOException {
        List<Employee> employees = new ArrayList<>();
        employees.add(new Employee(2L,"张三",1,25,"广州天河公园","java developer"));
        employees.add(new Employee(3L,"李四",1,28,"广州荔湾大厦","java assistant"));
        employees.add(new Employee(4L,"小红",0,26,"广州白云山公园","php developer"));

        List<BulkOperation> list = new ArrayList<>();
        for (Employee employee : employees) {
            BulkOperation bulkOperation = new BulkOperation.Builder()
                    .create(c->c.id(String.valueOf(employee.getId()))
                            .document(employee)
                    )
                    .build();

            list.add(bulkOperation);
        }

        //bulk批量插入文档
        elasticsearchClient.bulk(b->b.index(indexName).operations(list));
    }

    @Test
    public void testDocument() throws IOException {
        Employee employee = new Employee(12L,"张三三",1,25,"广州天河公园","java developer");

        IndexRequest<Employee> request = IndexRequest.of(i -> i
                .index(indexName)
                .id(employee.getId().toString())
                .document(employee)
        );

        IndexResponse response = elasticsearchClient.index(request);

        log.info("response:"+response);
    }

    @Test
    public void testQuery2() throws IOException {
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
        SearchRequest request  = new SearchRequest.Builder()
                .index("employee")
                .query(q->q.term(t->t.field("name").value("张三")))
                .build();
        SearchResponse<Employee> response = elasticsearchClient.search(request, Employee.class);

        List<Hit<Employee>> hits = response.hits().hits();
        hits.stream().forEach(employeeHit -> {
            log.info(employeeHit.source().toString());
        });

    }

    @Test
    public void testQuery3() throws IOException {
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
        SearchRequest request  = new SearchRequest.Builder()
                .index("employee")
                .query(q->q.bool(b->b.must(
                        QueryBuilders.match(q1->q1.field("address").query("广州")),
                        QueryBuilders.match(q1->q1.field("remark").query("java"))
                )))
                .build();
        log.info("构建的DSL语句："+ request.toString());

        SearchResponse<Employee> response = elasticsearchClient.search(request, Employee.class);

        List<Hit<Employee>> hits = response.hits().hits();
        hits.stream().forEach(employeeHit -> {
            log.info(employeeHit.source().toString());
        });

    }


    @Test
    public void testQuery() throws IOException {
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index(indexName)
                .query(q -> q.match(m -> m.field("name").query("张三"))
         ));

        log.info("构建的DSL语句:"+ searchRequest.toString());

        SearchResponse<Employee> searchResponse = elasticsearchClient.search(searchRequest, Employee.class);

        List<Hit<Employee>> hits = searchResponse.hits().hits();
        hits.stream().map(Hit::source).forEach(employee -> {
            log.info("员工信息:"+employee);
        });

    }


    @Test
    public void testBoolQueryDocument() throws IOException {
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
        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();
        boolQueryBuilder.must(m->m.match(q->q.field("address").query("广州")))
                .must(m->m.match(q->q.field("remark").query("java")));

        SearchRequest searchRequest = new SearchRequest.Builder()
                .index("employee")
                .query(q->q.bool(boolQueryBuilder.build()))
                .build();

        log.info("构建的DSL语句:"+ searchRequest.toString());

        //第二步：调用search查询
        SearchResponse<Employee> searchResponse = elasticsearchClient.search(searchRequest, Employee.class);
        //第三步：解析返回结果
        List<Hit<Employee>> list = searchResponse.hits().hits();
        for(Hit<Employee> hit: list){
            //返回source
            log.info(String.valueOf(hit.source()));
        }


    }

}
