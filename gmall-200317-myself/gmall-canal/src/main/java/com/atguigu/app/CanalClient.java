package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MykafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) {

        //获取Canal连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop202", 11111),
                "example",
                "", "");

        while (true){

            //连接
            canalConnector.connect();

            //订阅监控的表
            canalConnector.subscribe("gmall200317.*");

            // 抓取数据,不是非要等到100条才返回
            Message message = canalConnector.get(100);

            //判断当前是否抓取到数据
            if(message.getEntries().size() <= 0){
                System.out.println("当前抓取没有数据，休息一下！！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {

                // 1. 获取message中entry集合，并遍历
                for (CanalEntry.Entry entry : message.getEntries()) {

                    // 2. 获取entry中rowdata类型的数据
                    if(CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())){
                        try {
                        //3. 获取entry中的表名和数据
                        String tableName = entry.getHeader().getTableName();
                        ByteString storeValue = entry.getStoreValue();

                            // 4. 反序列化storeValue
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                            //5. 获取事件类型
                            CanalEntry.EventType eventType = rowChange.getEventType();

                            //6. 获取数据
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            handler(tableName,eventType,rowDatasList);


                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }



    //处理数据，根据表名以及事件类型将数据发送到kafka 判断主题
    private static void handler(String tableName,CanalEntry.EventType eventType,List<CanalEntry.RowData> rowDataList){

        //  将ordr_info表中的新增数据 写入kafka对应主题
        if("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            sendtokafka(rowDataList, GmallConstants.GMALL_TOPIC_ORDER_INFO);
        }else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            sendtokafka(rowDataList, GmallConstants.GMALL_TOPIC_ORDER_DETAIL);
        }else if("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType)
                ||CanalEntry.EventType.UPDATE.equals(eventType) )){
            sendtokafka(rowDataList, GmallConstants.GMALL_TOPIC_ORDER_DETAIL);
        }
    }

    private static void sendtokafka(List<CanalEntry.RowData> rowDataList, String topic) {
        for (CanalEntry.RowData rowData : rowDataList) {

            //创建json对象存放一行数据
            JSONObject jsonObject = new JSONObject();

            //遍历修改只有的列集
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            // 打印大巷数据并写入kafka
            System.out.println(jsonObject.toString());
            MykafkaSender.send(topic, jsonObject.toString());
        }
    }


}
