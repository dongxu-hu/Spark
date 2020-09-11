package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date){

        //1. 查询日活总数
        Integer dauTotal = publisherService.getDauTotal(date);
        Double sumTotal = publisherService.getsum_amount(date);

        //2. 创建list用于存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //3.1 创建map用于存放日活数据
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        //3.2 创建Map用于存放新增用户数据
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id","new_id");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);

        //3.3 创建map用于存放总数
        HashMap<String, Object> amoutMap = new HashMap<>();
        dauMap.put("id","order_amount");
        dauMap.put("name","新增交易额");
        dauMap.put("value",sumTotal);

        //4. 将Map放入集合
        result.add(dauMap);
        result.add(newMidMap);
        result.add(amoutMap);

        //5. 返回结果
        return JSONObject.toJSONString(result);
    }


    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id,@RequestParam("date") String date){
        // 创建Map用于存放结果数据
        HashMap<String,Map> result = new HashMap<>();
        Map todayMap = null;
        Map yesterdayMap = null;
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        if ("dau".equals(id)){
            // 1. 查询当天的分时数据
            todayMap =publisherService.getDauTotalHourMap(date);
            //2. 查询昨天的分时数据
            yesterdayMap = publisherService.getDauTotalHourMap(yesterday);
        }else if("new_id".equals(id)){
            yesterdayMap.put("09",100L);
            yesterdayMap.put("12",200L);
            yesterdayMap.put("14",300L);
            todayMap.put("08",400L);
            todayMap.put("15",500L);
            todayMap.put("17",600L);
        }else if("order_amout".equals(id)){
            // 1. 查询当天的分时数据
            todayMap = publisherService.getorderAmountHour(date);
            //2. 查询昨天的分时数据
            yesterdayMap = publisherService.getorderAmountHour(yesterday);
        }
        //3. 将昨天数据、今天数据放入结果map中
        result.put("yesterday",yesterdayMap);
        result.put("today",todayMap);

        return JSONObject.toJSONString(result);
    }

}
