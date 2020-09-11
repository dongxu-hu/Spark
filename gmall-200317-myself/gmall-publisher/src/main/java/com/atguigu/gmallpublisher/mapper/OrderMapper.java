package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    // 获取Phoenix表中的GMv
    public Double selectOrderAmountTotal (String date);

    // 获取日活数据查询接口数据
    public List<Map> selectOrderAmountHourMap(String date);

}
