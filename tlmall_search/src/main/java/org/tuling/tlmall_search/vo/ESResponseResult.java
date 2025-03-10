package org.tuling.tlmall_search.vo;


import lombok.Data;
import org.tuling.tlmall_search.domain.EsProduct;

import java.util.List;
import java.util.Objects;

@Data
public class ESResponseResult {

    /**
     * 查询到的所有商品信息
     */
    private List<EsProduct> products;


    /**
     * 当前页码
     */
    private Integer pageNum;

    /**
     * 总记录数
     */
    private Long total;

    /**
     * 总页码
     */
    private Integer totalPages;

    private List<Integer> pageNavs;

    /**
     * 当前查询到的结果，所有涉及到的品牌
     */
    private List<BrandVo> brands;

    /**
     * 当前查询到的结果，所有涉及到的所有属性
     */
    private List<AttrVo> attrs;

    /**
     * 当前查询到的结果，所有涉及到的所有分类
     */
    private List<categoryVo> categorys;



    //===========================以上是返回给页面的所有信息============================//

    @Data
    public static class NavVo {
        private String navName;
        private String navValue;
        private String link;

    }


    @Data
    public static class BrandVo {

        private Long brandId;

        private String brandName;

        private String brandImg;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BrandVo brandVo = (BrandVo) o;
            return Objects.equals(brandId, brandVo.brandId) &&
                    Objects.equals(brandName, brandVo.brandName) &&
                    Objects.equals(brandImg, brandVo.brandImg);
        }

        @Override
        public int hashCode() {
            return Objects.hash(brandId, brandName, brandImg);
        }
    }


    @Data
    public static class AttrVo {

        private Long attrId;

        private String attrName;

        private List<String> attrValue;

    }


    @Data
    public static class categoryVo {

        private Long categoryId;

        private String categoryName;

    }
}
