package com.lyc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @ProjectName flume-interceptor
 * @ClassName MyHbaseSink
 * @Description TODO
 * @Author lyc
 * @Date 2022/5/21 11:46
 * @Version 1.0
 **/
public class MyHbaseSink extends AbstractSink implements Configurable {

    public static Configuration configuration;
    public static Connection connection;
    public static HBaseAdmin admin;
    public static String zookeeperQuorum;
    //zookeeper端口号
    public static String port;
    //hbase表名
    public static String tableName;
    //habse列簇
    public static String columnFamily;
    public static Table table;
    //线程池
    public static ExecutorService pool = Executors.newScheduledThreadPool(20);
    public static final Logger logger = LoggerFactory.getLogger(MyHbaseSink.class);



    @Override
    public void configure(Context context) {
        zookeeperQuorum = context.getString("zookeeperQuorum");
        port = context.getString("port");
        tableName = context.getString("tableName");
        columnFamily = context.getString("columnFamily");
        try {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.property.clientPort", port);
            configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
            connection = ConnectionFactory.createConnection(configuration, pool);
            admin = (HBaseAdmin)connection.getAdmin();
        } catch (IOException e) {
            logger.info("==============Hbase连接异常==============");
        }
    }

    @Override
    public Status process(){
        Status status = null;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        //开启事务
        transaction.begin();
        try {
            Event event;
            while(true) {
                event = channel.take();
                if (event != null) {
                    logger.info("==============获取到event==============");
                    break;
                }
            }
            processEvent(event);
            //提交事务
            transaction.commit();
            status = Status.READY;
        } catch (ChannelException e) {
            //事务回滚
            transaction.rollback();
            logger.info("==============插入数据失败==============");
            status = Status.BACKOFF;
        } finally {
            transaction.close();
            logger.info("==============插入数据成功==============");
        }
        return status;
    }

    public void processEvent(Event event) {
        String body = new String(event.getBody());
        JSONObject jsonObject = (JSONObject) JSON.parse(body);
        logger.info("======================================================================");
        logger.info("==============获取Kafka数据：" + jsonObject.toString() + "==============");
        logger.info("======================================================================");
        String rowkey = UUID.randomUUID().toString().replaceAll("-","");
        Put put = new Put(Bytes.toBytes(rowkey));
        ArrayList<Put> putlist = new ArrayList<>();
        jsonObject = (JSONObject)jsonObject.get("Document");
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            System.out.println(entry.getKey() + "，" + entry.getValue());
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
            putlist.add(put);
        }
        putBatch(tableName, putlist);
    }

    /**
     * @Description: TODO 批量插入数据
     * @Author: lyc
     * @Date: 2022/5/22 11:55
     * @param tableName:
     * @param putList:
     * @return: void
     **/
    public void putBatch(String tableName, List<Put> putList) {
        if (tableExists(tableName)) {
            try {
                table = connection.getTable(TableName.valueOf(tableName));
                table.put(putList);
                if (System.currentTimeMillis() % 20 == 1) {
                    admin.flush(TableName.valueOf(tableName));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                closeTable(table);
            }
        }else {
            throw new IllegalArgumentException(tableName + "表不存在");
        }

    }

    /**
     * @Description: TODO 判断表是否存在
     * @Author: lyc
     * @Date: 2022/5/22 11:56
     * @param tableName:
     * @return: boolean
     **/
    public boolean tableExists(String tableName) {
        TableName table = TableName.valueOf(tableName);
        boolean tableExistsFlag = false;
        try {
            tableExistsFlag = admin.tableExists(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tableExistsFlag;
    }

    /**
     * @Description: TODO 关闭流
     * @Author: lyc
     * @Date: 2022/5/22 11:57
     * @param table:
     * @return: void
     **/
    public void closeTable(Table table) {
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
