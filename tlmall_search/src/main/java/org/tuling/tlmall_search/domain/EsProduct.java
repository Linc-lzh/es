package org.tuling.tlmall_search.domain;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

/**
 * 搜索中的商品信息
 */
@Data
@Document(indexName = "product_db")
public class EsProduct implements Serializable {
    private static final long serialVersionUID = -1L;
    @Id
    private Long id;
    /** 商品名称*/
    @Field(analyzer = "ik_max_word", type = FieldType.Text)
    private String name;
    /** 商品关键词 */
    @Field(analyzer = "ik_max_word", type = FieldType.Text)
    private String keywords;
    /** 商品副标题 */
    @Field(analyzer = "ik_max_word", type = FieldType.Text)
    private String subTitle;
    /** 当前售卖的价格 */
    private BigDecimal price;
    /** 促销价格 */
    private BigDecimal promotionPrice;
    /** 出厂价格 */
    private BigDecimal originalPrice;
    /** 商品图片*/
    @Field(type = FieldType.Keyword)
    private String pic;
    /** 展示的销量 */
    private Integer sale;
    /** 当前实际的销量 */
    private Integer salecount;
    /** 库存状态，是否有库存，true表示有库存，false表示无库存 */
    @Field(type = FieldType.Boolean)
    private boolean hasStock;
    /**上架日期*/
    @Field(type = FieldType.Date,pattern = "yyyy-MM-dd")
    private String putawayDate;
    /**品牌ID */
    private Long brandId;
    /** 品牌名称 */
    @Field(type = FieldType.Keyword)
    private String brandName;
    /** 品牌图片 */
    @Field(type = FieldType.Keyword)
    private String brandImg;
    /**商品分类ID*/
    private Long categoryId;
    /**商品分类名称*/
    @Field(type = FieldType.Keyword)
    private String categoryName;

    /** 商品属性,一个数组，包含了产品的多个属性，每个属性是一个对象*/
    @Field(type = FieldType.Nested)
    private List<Attribute> attrs;


}

@Data
class Attribute {
    /** 属性ID*/
    private long attrId;
    /**属性名称，如“容量”或“网络” */
    @Field(type = FieldType.Keyword)
    private String attrName;
    /** 属性值，如“128G”或“5G” */
    @Field(type = FieldType.Keyword)
    private String attrValue;
}

