package org.tuling.vip_es_demo;


import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.tuling.vip_es_demo.bean.Employee;
import org.tuling.vip_es_demo.bean.EmployeeQueryParameter;
import org.tuling.vip_es_demo.dao.EmployeeRepository;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;


@Slf4j
public class ElasticsearchRepositoryTest extends VipEsDemoApplicationTests {

    @Autowired
    EmployeeRepository employeeRepository;


    @Test
    public void test(){

//        List<Employee> list = employeeRepository.findByName("张三");
//        for (Employee employee:list){
//            log.info(employee.toString());
//        }

        Optional<Employee> e = employeeRepository.findById(2L);
        log.info(String.valueOf(e.get()));


    }


    @Test
    public void testDocument() {

        Employee employee = new Employee(10L, "fox666", 1, 32, "长沙麓谷", "java architect");
        //插入文档
        employeeRepository.save(employee);

        //根据id查询
        Optional<Employee> result = employeeRepository.findById(10L);
        if (!result.isEmpty()){
            log.info(String.valueOf(result.get()));
        }

        //根据name查询
        List<Employee> list = employeeRepository.findByName("fox666");
        if(!list.isEmpty()){
            log.info(String.valueOf(list.get(0)));
        }

    }


    @Test
    public void testGetByIds(){
        Long[] ids = {1L,2L,3L};
        List<Employee> list = employeeRepository.getByIds(Arrays.asList(ids));
        if(!list.isEmpty()){
            log.info(String.valueOf(list));
        }
    }


    @Test
    public void testFindByAddress(){
        Page<Employee> page = employeeRepository.findByAddress("广州",Pageable.ofSize(3));
//        // 获取分页数据中的Employee列表
        List<Employee> list = page.getContent();
        if(!list.isEmpty()){
            log.info(String.valueOf(list));
        }

    }


    @Test
    public void testFindByName(){

        Page<Employee> page = employeeRepository.findByName("张三",Pageable.ofSize(3));
        // 获取分页数据中的Employee列表
        List<Employee> list = page.getContent();
        if(!list.isEmpty()){
            log.info(String.valueOf(list));
        }

    }


    @Test
    public void testFindByName2(){
        EmployeeQueryParameter employeeQueryParameter = new EmployeeQueryParameter("李四");
        Page<Employee> page = employeeRepository.findByName(employeeQueryParameter,Pageable.ofSize(1));
        // 获取分页数据中的Employee列表
        List<Employee> list = page.getContent();
        if(!list.isEmpty()){
            log.info(String.valueOf(list));
        }

    }

}
