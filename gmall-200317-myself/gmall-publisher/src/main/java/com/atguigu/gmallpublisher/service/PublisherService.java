package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {

    public Integer getDauTotal(String date);

    public Map getDauTotalHourMap(String date);

    public Double getsum_amount(String date);

    public Map getorderAmountHour(String date);

    public Map getSaleDetail(String date,int starpage, int size,String  keyword );

}
