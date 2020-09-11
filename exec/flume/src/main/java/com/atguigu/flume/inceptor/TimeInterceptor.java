package com.atguigu.flume.inceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;


import java.nio.charset.StandardCharsets;
import java.util.List;


public class TimeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String logs = new String(body, StandardCharsets.UTF_8);

        JSONObject jsonObject = JSON.parseObject(logs);
        if(jsonObject.containsKey("ts")){
            String ts = jsonObject.getString("ts");
            event.getHeaders().put("timestamp",ts);
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        for (Event event : list) {
            intercept(event);
        }

        return list;
    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TimeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

    @Override
    public void close() {

    }
}
