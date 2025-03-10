package org.tuling.tlmall_search.vo;



import lombok.Data;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Data
public class ESRequestParam {

    /**
     * 页面传递过来的全文匹配关键字
     */
    private String keyword;

    /**
     * 品牌id,可以多选
     */
    private List<Long> brandId;

    /**
     * 分类id
     */
    private Long categoryId;

    /**
     * 排序条件：sort=price/salecount/hotscore_desc/asc
     */
    private String sort;

    private Long salecount;//销量

    private Date putawayDate;//上架时间
    /**
     * 是否显示有货  1代表有货
     */
    private Integer hasStock;

    /**
     * 价格区间查询
     */
    private String price;

    /**
     * 按照属性进行筛选
     */
    private List<String> attrs;

    /**
     * 页码
     */
    private Integer pageNum = 1;

    /**
     * 原生的所有查询条件
     */
    private String queryString;




}
