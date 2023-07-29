package com.tuling.vipes;

import com.tuling.vipes.bean.Employee;
import com.tuling.vipes.dao.EmployeeRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Optional;

@Slf4j
public class ElasticsearchRepositoryTest extends VipEsApplicationTests {

    @Autowired
    EmployeeRepository employeeRepository;

    @Test
    public void testDocument() {

        Employee employee = new Employee(1L, "fox666", 1, 32, "长沙麓谷", "java architect");
        //插入文档
        employeeRepository.save(employee);

        //根据id查询
        Optional<Employee> result = employeeRepository.findById(1L);
        log.info(String.valueOf(result.get()));

        //根据name查询
        List<Employee> list = employeeRepository.findByName("fox666");
        log.info(String.valueOf(list.get(0)));

    }

}
