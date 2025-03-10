package org.tuling.tlmall_search.service;


import org.tuling.tlmall_search.vo.ESRequestParam;
import org.tuling.tlmall_search.vo.ESResponseResult;

public interface TulingMallSearchService {


    /**
     * @param param 检索的所有参数
     * @return  返回检索的结果，里面包含页面需要的所有信息
     */
    ESResponseResult search(ESRequestParam param);


}


