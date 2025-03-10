package org.tuling.tlmall_search;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.util.ObjectBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.HighlightQuery;
import org.springframework.data.elasticsearch.core.query.Order;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.data.elasticsearch.core.query.StringQuery;
import org.springframework.data.elasticsearch.core.query.highlight.Highlight;
import org.springframework.data.elasticsearch.core.query.highlight.HighlightField;
import org.springframework.data.elasticsearch.core.query.highlight.HighlightParameters;
import org.springframework.util.StringUtils;
import org.tuling.tlmall_search.common.SearchConstant;
import org.tuling.tlmall_search.domain.EsProduct;
import org.tuling.tlmall_search.vo.ESResponseResult;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@SpringBootTest
@Slf4j
class TlmallSearchApplicationTests {


    @Autowired
    ElasticsearchTemplate elasticsearchTemplate;
    private ChildrenAggregation.Builder brandAgg;
    private ParentAggregation brandAgg1;

    /*
 POST /product_db/_search
{
  "from": 0,
  "size": 8,
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "name": {
              "query": "华为手机"
            }
          }
        }
      ]
    }
  },
  "highlight": {
    "pre_tags": [
      "<b style='color:red'>"
    ],
    "post_tags": [
      "</b>"
    ],
    "fields": {
      "name": {}
    }
  }
}
     */

    @Test
    public void testQuery(){
        String text = "华为手机";

        //构建查询语句
        Query query = NativeQuery.builder()
                .withQuery(QueryBuilders.bool(b->b.must(
                    //Query
                    m -> m.match(q -> q.field("name").query(text))
                )))
                .build();

        //分页  注意：from = pageNumber（页码，从0开始，） * pageSize（每页的记录数）
        query.setPageable(PageRequest.of(0, 8));
        //高亮
        HighlightField highlightField = new HighlightField("name");
        HighlightParameters highlightParameters = new HighlightParameters.HighlightParametersBuilder()
                .withPreTags("<b style='color:red'>")
                .withPostTags("</b>")
                .build();
        Highlight highlight = new Highlight(highlightParameters, Arrays.asList(highlightField));
        HighlightQuery highlightQuery = new HighlightQuery(highlight,EsProduct.class);
        query.setHighlightQuery(highlightQuery);

        //查询
        SearchHits<EsProduct> searchHits = elasticsearchTemplate.search(query, EsProduct.class);

        for (SearchHit hit: searchHits){
            log.info("返回结果："+hit.toString());
        }

    }

    @Qualifier("elasticsearchClient")
    @Autowired
    ElasticsearchClient client;

    @Test
    public void testSearch() throws IOException, ElasticsearchException {
        String text = "华为手机";

        SearchRequest.Builder searchRequestBuilder = new SearchRequest.Builder();

        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();

        //单字段
//        boolQueryBuilder
//                .must( //Query
//                        m -> m.match(q -> q.field("name").query(text)
//                ));
        //多字段
        boolQueryBuilder.must(m->m.multiMatch(
                q->q.fields("name", "keywords", "subTitle").query(text)
        ));

        //terms
        List<Long> brandIdList = Arrays.asList(1L,3L);
        List<FieldValue> brandIds = brandIdList.stream()
                .map(b-> FieldValue.of(b))
                .collect(Collectors.toList());
        boolQueryBuilder.filter(QueryBuilders.terms(
                t->t.field("brandId").terms(v->v.value(brandIds))
        ));

        //nested查询
        //attrs=1_64G
        BoolQuery.Builder boolQuery = QueryBuilders.bool();
        String[] str = "1_64G".split("_");
        String attrId = str[0];
        String[] attrValues = str[1].split(":");//这个属性检索用的值

        boolQuery.filter(QueryBuilders.term(
                t->t.field("attrs.attrId").value(attrId)
        ));

        List<FieldValue> attrValueList = Arrays.stream(attrValues)
                .map(b-> FieldValue.of(b))
                .collect(Collectors.toList());
        boolQuery.filter(QueryBuilders.terms(
                t->t.field("attrs.attrValue").terms(v->v.value(attrValueList))
        ));

        NestedQuery.Builder nestedQueryBuilder = new NestedQuery.Builder();

        nestedQueryBuilder.path("attrs").query(q->q.bool(boolQuery.build())).scoreMode(ChildScoreMode.None);

        boolQueryBuilder.filter(q->q.nested(nestedQueryBuilder.build()));

        /**
         * 对品牌、分类信息、属性信息进行聚合分析
         */

        //1. 按照品牌进行聚合
        //1.1 品牌的子聚合-品牌名聚合
        Aggregation brand_name_agg =
                AggregationBuilders.terms(t -> t.field("brandName").size(1));
        //1.2 品牌的子聚合-品牌图片聚合
        Aggregation brand_img_agg =
                AggregationBuilders.terms(t -> t.field("brandImg").size(1));

        Aggregation brand_agg = new Aggregation.Builder()
                //按照品牌id进行聚合
                .terms(t -> t.field("brandId").size(50))
                .aggregations("brand_name_agg", brand_name_agg)
                .aggregations("brand_img_agg", brand_img_agg)
                .build();

        searchRequestBuilder.aggregations("brand_agg",brand_agg);



        //2. 按照属性信息进行聚合
        NestedAggregation attrs = new NestedAggregation.Builder()
                .path("attrs")
                .build();

        Aggregation attr_id_agg = new Aggregation.Builder()
                //2.1 按照属性ID进行聚合
                .terms(t -> t.field("attrs.attrId"))
                //2.1.1 在每个属性ID下，按照属性名进行聚合
                .aggregations("attr_name_agg", AggregationBuilders.terms(t -> t.field("attrs.attrName").size(1)))
                //2.1.1 在每个属性ID下，按照属性值进行聚合
                .aggregations("attr_value_agg", AggregationBuilders.terms(t -> t.field("attrs.attrValue").size(1)))
                .build();

        Aggregation attrs_agg = new Aggregation.Builder()
                .nested(attrs)
                .aggregations("attr_id_agg", attr_id_agg)
                .build();

        searchRequestBuilder.aggregations("attrs_agg",attrs_agg);


        //排序
        FieldSort sortOptions = SortOptionsBuilders.field().field("price").order(SortOrder.Desc).build();


        //高亮显示
        co.elastic.clients.elasticsearch.core.search.HighlightField highlightField = new co.elastic.clients.elasticsearch.core.search.HighlightField.Builder()
                .preTags("<b style='color:red'>")
                .postTags("</b>")
                .build();

        //构建查询
        SearchRequest searchRequest = searchRequestBuilder
                .index(SearchConstant.INDEX_NAME)
                .query(q -> q.bool(boolQueryBuilder.build()))
                .sort(s->s.field(sortOptions))
                .highlight(h->h.fields("name",highlightField))
                .build();

        log.info("构建的DSL语句:"+ searchRequest.toString());

        SearchResponse response = client.search(searchRequest, EsProduct.class);

        log.info("response:"+response);

        //获取查询到的商品信息
        HitsMetadata<EsProduct> hitsMetadata = response.hits();
        List<Hit<EsProduct>> hits = hitsMetadata.hits();

        if(!hits.isEmpty()){
            for (Hit<EsProduct> hit : hits){
                //获取商品信息
                EsProduct product = hit.source();
                // 拿到高亮信息显示标题
                List<String> name = hit.highlight().get("name");
                //判断name中是否含有查询的关键字(因为是多字段查询，因此可能不包含指定的关键字，假设不包含则显示原始name字段的信息)
                String nameValue = name != null ? name.get(0) : product.getName();
                product.setName(nameValue);

                log.info("product:"+product.toString());

            }
        }


        // 获取聚合结果
        Map<String, Aggregate> aggs = response.aggregations();
        //获取到品牌的聚合
        Aggregate brandAgg = aggs.get("brand_agg");
        if(brandAgg != null){
            List<LongTermsBucket> brandIdBuckets = brandAgg.lterms().buckets().array();
            for (LongTermsBucket brandIdBucket:brandIdBuckets) {
                //构建品牌信息
                ESResponseResult.BrandVo brandVo = new ESResponseResult.BrandVo();
                //获取品牌ID
                brandVo.setBrandId(brandIdBucket.key());

                Aggregate brandImgAgg = brandIdBucket.aggregations().get("brand_img_agg");
                Aggregate brandNameAgg = brandIdBucket.aggregations().get("brand_name_agg");
                if (brandImgAgg != null && brandNameAgg != null) {
                    StringTermsBucket imgBucket = brandImgAgg.sterms().buckets().array().get(0);
                    StringTermsBucket nameBucket = brandNameAgg.sterms().buckets().array().get(0);
                    //设置品牌的图片和名称
                    brandVo.setBrandImg(imgBucket.key().stringValue());
                    brandVo.setBrandName(nameBucket.key().stringValue());
                }
                log.info("品牌："+brandVo.toString());
            }
        }




    }


    @Test
    public void test() throws IOException {

        //构建DSL
        // bool
        BoolQuery.Builder boolQuerybuilder = new BoolQuery.Builder();
        //关键词搜索 multi_match
        boolQuerybuilder.must(QueryBuilders.multiMatch(
           m->m.fields("name","keyword","subTitle").query("手机")
        ));
        //价格  range
        boolQuerybuilder.filter(
                QueryBuilders.range(
                    r->r.field("price").gte(JsonData.of("1000")).lte(JsonData.of("10000")
                )),
                QueryBuilders.term(t->t.field("hasStock").value("true"))
        );

        //查商品属性

        BoolQuery.Builder boolQuery = new BoolQuery.Builder()
                .filter(f->f.term(t->t.field("attrs.attrId").value("2")))
                .filter(f->f.term(t->t.field("attrs.attrValue").value("5G")));
        NestedQuery.Builder nestedQueryBuilder = new NestedQuery.Builder()
                .path("attrs")
                .query(q->q.bool(boolQuery.build()));

        //对品牌的聚合
        Aggregation brand_agg = new Aggregation.Builder()
                .terms(t->t.field("brandId").size(20))
                .aggregations("brandName_agg",AggregationBuilders.terms(t->t.field("brandName").size(1)))
                .aggregations("brandImg_agg",AggregationBuilders.terms(t->t.field("brandImg").size(1)))
                .build();

        //对商品属性聚合
        Aggregation attr_id_agg = new Aggregation.Builder()
                .terms(t -> t.field("attrs.attrId"))
                .aggregations("attrName_agg",AggregationBuilders.terms(t->t.field("attrs.attrName").size(1)))
                .aggregations("attrValue_agg",AggregationBuilders.terms(t->t.field("attrs.attrValue").size(1)))
                .build();

        Aggregation attrs_agg = new Aggregation.Builder()
                .nested(n->n.path("attrs"))
                .aggregations("attr_id_agg",attr_id_agg)
                .build();

        boolQuerybuilder.filter(f->f.nested(nestedQueryBuilder.build()));
        //高亮显示
        co.elastic.clients.elasticsearch.core.search.HighlightField highlightField = new co.elastic.clients.elasticsearch.core.search.HighlightField.Builder().preTags("<b style='color:red'>").postTags("</b>").build();

        SearchRequest searchRequest = new SearchRequest.Builder()
                .index("product_db")
                .query(q->q.bool(boolQuerybuilder.build()))
                .aggregations("brand_agg",brand_agg)
                .aggregations("attrs_agg",attrs_agg)
                .highlight(h -> h.fields("name", highlightField))
                .build();



        log.info("构建的DSL："+searchRequest.toString());

        SearchResponse response = client.search(searchRequest, EsProduct.class);

        ESResponseResult esResponseResult = new ESResponseResult();
        List<EsProduct> products = new ArrayList<>();


        HitsMetadata hits = response.hits();

        List<Hit<EsProduct>> hits1 = hits.hits();
        for (Hit<EsProduct> hit: hits1){
            EsProduct product = hit.source();

            product.setName(hit.highlight().get("name").toString());
            products.add(product);
            log.info("product："+product.toString());
        }
        esResponseResult.setProducts(products);

        List<ESResponseResult.BrandVo> brands = new ArrayList<>();


        Map<String, Aggregate> aggregations = response.aggregations();
        //品牌解析
        Aggregate brand_aggs = aggregations.get("brand_agg");

        List<LongTermsBucket> brandArray = brand_aggs.lterms().buckets().array();
        for (LongTermsBucket bucket: brandArray){
            ESResponseResult.BrandVo brandVo = new ESResponseResult.BrandVo();
            //设置品牌ID
            brandVo.setBrandId(bucket.key());
            Aggregate brandNameAgg = bucket.aggregations().get("brandName_agg");
            StringTermsBucket brandNameBucket = brandNameAgg.sterms().buckets().array().get(0);
            //设置品牌名
            brandVo.setBrandName(brandNameBucket.key().stringValue());
            Aggregate brandImgAgg = bucket.aggregations().get("brandImg_agg");
            StringTermsBucket brandImgBucket = brandImgAgg.sterms().buckets().array().get(0);
            //设置品牌名
            brandVo.setBrandName(brandImgBucket.key().stringValue());
            brands.add(brandVo);
        }
        esResponseResult.setBrands(brands);
        //商品属性解析
        List<ESResponseResult.AttrVo> attrs = new ArrayList<>();
        Aggregate attrs_aggs = aggregations.get("attrs_agg");
        Aggregate attrIdAgg = attrs_aggs.nested().aggregations().get("attr_id_agg");
        List<LongTermsBucket> attrIdArray = attrIdAgg.lterms().buckets().array();
        for (LongTermsBucket bucket: attrIdArray){
            ESResponseResult.AttrVo attrVo = new ESResponseResult.AttrVo();
            //设置属性ID
            attrVo.setAttrId(bucket.key());

            Aggregate attrNameAgg = bucket.aggregations().get("attrName_agg");
            StringTermsBucket attrNameBucket = attrNameAgg.sterms().buckets().array().get(0);
            //设置属性名称
            attrVo.setAttrName(attrNameBucket.key().stringValue());
            Aggregate attrValueAgg = bucket.aggregations().get("attrValue_agg");
            List<StringTermsBucket> attrValueArray = attrValueAgg.sterms().buckets().array();
            List<String> attrValues = new ArrayList<>();
            for (StringTermsBucket attrValuebucket:attrValueArray){
                attrValues.add(attrValuebucket.key().stringValue());
            }
            //设置属性值
            attrVo.setAttrValue(attrValues);

            log.info("attrVo: "+attrVo.toString());
            attrs.add(attrVo);
        }
        esResponseResult.setAttrs(attrs);



    }




}


