package org.tuling.vip_es_demo.dao;


import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;
import org.tuling.vip_es_demo.bean.Employee;
import org.tuling.vip_es_demo.bean.EmployeeQueryParameter;

import java.awt.print.Book;
import java.util.Collection;
import java.util.List;

@Repository
public interface EmployeeRepository extends ElasticsearchRepository<Employee, Long> {

    //等同于 { "query" : { "bool" : { "must" : [ { "query_string" : { "query" : "?", "fields" : [ "name" ] } } ] } }}
    List<Employee> findByName(String name);


    //等同于：
    // {
    //  "query": {
    //    "ids": {
    //      "values": ["id1", "id2", "id3"]
    //    }
    //  }
    //}
    @Query("{\"ids\": {\"values\": ?0 }}")
    List<Employee> getByIds(Collection<Long> ids);

    //等同于
    // {
    //  "query": {
    //    "match": {
    //      "address": {
    //        "query": "广州"
    //      }
    //    }
    //  }
    //}
    @Query("{\"match\": {\"address\": {\"query\": \"?0\"}}}")
    Page<Employee> findByAddress(String address1, Pageable pageable);


    @Query("""
            {
              "bool":{
                "must":[
                  {
                    "term":{
                      "name": "#{#name}"
                    }
                  }
                ]
              }
            }
            """)
    Page<Employee> findByName(String name, Pageable pageable);


    @Query("""
            {
              "bool":{
                "must":[
                  {
                    "term":{
                      "name": "#{#parameter.keyword}"
                    }
                  }
                ]
              }
            }
            """)
    Page<Employee> findByName(EmployeeQueryParameter parameter, Pageable pageable);

}