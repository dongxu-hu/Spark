package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;

import com.atguigu.gmallpublisher.service.bean.Option;
import com.atguigu.gmallpublisher.service.bean.Stat;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService{

    //调用mapper的实现类
    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    //获取日活总数
    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    //  创建Map用于存放调整结构的数据
    HashMap result = new HashMap<>();

    // 获取分时数据
    @Override
    public Map getDauTotalHourMap(String date) {

        // 1. 查询phoenix数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        // 3. 调整结构
        for (Map map : list) {

            // LH 和 CT 是可视化展示中定义值
            result.put((String)map.get("LH"), (Long)map.get("CT"));
        }

        return result;
    }

    //获取GAV
    @Override
    public Double getsum_amount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    // 获取分时数据
    @Override
    public Map getorderAmountHour(String date) {

        //1. 查询phoenix数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //3. 调整结构
        for (Map map : list) {
            result.put((String)map.get("LH"), (Long)map.get("CT"));
        }
        return result;
    }

    @Override
    public Map getSaleDetail(String date, int starpage,  int size,String keyword) {

        //1. 创建DSL语句构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //1.1 查询条件相关
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //1.1.1 添加全值匹配选项
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("dt", date);
        BoolQueryBuilder filter = boolQueryBuilder.filter(termQueryBuilder);
        // 1.1.2 条件分词匹配
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND);
        boolQueryBuilder.must(matchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);

        // 1.2 添加聚合组
        TermsBuilder ageAggs = AggregationBuilders.terms("groupByAge").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);
        TermsBuilder genderAggs = AggregationBuilders.terms("groupByGender").field("user_gender").size(3);
        searchSourceBuilder.aggregation(genderAggs);

        //1.3 分页相关
        searchSourceBuilder.from((starpage -1)* size);
        searchSourceBuilder.size(size);

        // 2. 创建search对象
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("")
                .addType("")
                .build();


        // 3. 执行查询
        SearchResult searchResult = null;
        try {
            searchResult = jestClient.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //4. 解析searchResult
        // 4.1 定义map用于存放最终的结果集
        HashMap<String, Object> result = new HashMap<>();

        //4.2 向结果集中添加总数
        assert searchResult !=null;   //断言结果不为null
        Long total = searchResult.getTotal();

        //4.3 封装年龄及性别聚合组
        MetricAggregation aggregations = searchResult.getAggregations();
        //定义list结合接收明细结果
        List<Stat> stats =new ArrayList<>();


        // 4.3.1 年龄聚合组
        TermsAggregation groupByAge = aggregations.getTermsAggregation("groupByAge");

        //定义每个年龄段人数
        Long lower20 = 0L;
        Long uper20to30 = 0L;

        //获取各个年龄段人数
        for (TermsAggregation.Entry bucket : groupByAge.getBuckets()) {
            long age = Long.parseLong(bucket.getKey());
            if(age<20){
                lower20 += bucket.getCount();
            }else if(age <30){
                uper20to30 += bucket.getCount();
            }
        }
        // 获取各个年龄段的比例
        double low20Ratio = Math.round(lower20 * 1000D / total) / 10D;
        double uper20to30Ratio = Math.round(uper20to30 * 1000D / total) / 10D;
        double low30Ratio = 100D - low20Ratio - uper20to30Ratio;

        //创建3个年龄段的Option对象
        Option lower20Opt = new Option("20岁以下", low20Ratio);
        Option upper20to30Opt = new Option("20岁到30岁", uper20to30Ratio);
        Option upper30Opt = new Option("30岁及30岁以上", low30Ratio);

        //创建年龄的Stat对象
        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(lower20Opt);
        ageOptions.add(upper20to30Opt);
        ageOptions.add(upper30Opt);
        Stat ageStat = new Stat("用户年龄占比",ageOptions);

        //将年龄的饼图数据添加至集合
        stats.add(ageStat);

        //4.3.2 性别聚合组
        TermsAggregation groupByGender = aggregations.getTermsAggregation("groupByGender");
        Long femaleCount = 0L;

        //遍历聚合组中性别数据
        for (TermsAggregation.Entry entry : groupByGender.getBuckets()) {
            if ("F".equals(entry.getKey())) {
                femaleCount += entry.getCount();
            }
        }

        //计算男女性别比例
        double femaleRatio = (Math.round(femaleCount * 1000D / total)) / 10D;
        double maleRatio = 100D - femaleRatio;

        //创建2个性别Option对象
        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);

        //创建性别的Stat对象
        ArrayList<Option> genderOptions = new ArrayList<>();
        genderOptions.add(maleOpt);
        genderOptions.add(femaleOpt);
        Stat genderStat = new Stat("用户性别占比",genderOptions);

        //将用户性别饼图数据添加至集合
        stats.add(genderStat);

        //4.4 向结果集中添加明细数据
        ArrayList<Map> details = new ArrayList<>();
        //4.4.1 获取明细数据
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }

        //将准备的数据放入结果集
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", details);

        //5.将封装好的数据返回
        return result;
    }
}