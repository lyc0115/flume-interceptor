package com.lyc;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.http.StatusLine;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import java.io.File;
import java.util.HashMap;
import java.util.List;

/**
 * @ProjectName flume-interceptor
 * @ClassName XmlDirPollSource
 * @Description TODO
 * @Author lyc
 * @Date 2022/5/21 6:52
 * @Version 1.0
 **/
public class XmlDirPollSource extends AbstractSource implements Configurable, PollableSource {
    private Long delay; //检查时间间隔
    private String xmlSourceDir; //xml数据存放目录位置
    private String fileFilterSuffix; //过滤文件名后缀

    /**
     * @Description: TODO 获取配置文件参数
     * @Author: lyc
     * @Date: 2022/5/22 10:59
     * @param context:
     * @return: void
     **/
    @Override
    public void configure(Context context) {
        delay = context.getLong("delay");
        xmlSourceDir = context.getString("xmlSourceDir");
        fileFilterSuffix = context.getString("fileFilterSuffix");
    }

    /**
     * @Description: TODO 将json数据封装成event，对执行过的数据进行标记
     * @Author: lyc
     * @Date: 2022/5/22 10:59
     * @return: org.apache.flume.PollableSource.Status
     **/
    @Override
    public Status process() throws EventDeliveryException {
        String[] fileList = listFliles(xmlSourceDir);
        Status status = null;

        for(String fileName : fileList) {
            if (fileName.endsWith(fileFilterSuffix)) {
                SimpleEvent event = new SimpleEvent();
                HashMap<String, String> headerMap = new HashMap<>();
                headerMap.put("filename", fileName);
                try {
                    JSONObject jsonObject = xmltoJson(xmlSourceDir + "/" + fileName);
                    event.setHeaders(headerMap);
                    event.setBody(jsonObject.toString().getBytes());
                    getChannelProcessor().processEvent(event);

                    File file = new File(xmlSourceDir + "/" + fileName);
                    File dest = new File(file.getPath() + ".complete");
                    //执行过的文件打标记
                    file.renameTo(dest);
                    Thread.sleep(delay);
                    status = Status.READY;

                } catch (Exception e) {
                    e.printStackTrace();
                    status = Status.BACKOFF;
                }

            }
        }
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    public String[] listFliles(String path) {
        File file = new File(path);
        return file.list();
    }
    /**
     * @Description: TODO xml数据转换为Json数据
     * @Author: lyc
     * @Date: 2022/5/22 10:57
     * @param xml: xml文件路径
     * @return: com.alibaba.fastjson.JSONObject
     **/
    public JSONObject xmltoJson(String xml) throws Exception {
        JSONObject jsonObject = new JSONObject();
        File file = new File(xml);
        SAXReader saxReader = new SAXReader();
        saxReader.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        Document document = saxReader.read(file);
        Element root = document.getRootElement();
        iterateNodes(root, jsonObject);
        return jsonObject;
    }

    /**
     * @Description:TODO 解析xml数据
     * @Author: lyc
     * @Date: 2022/5/22 10:58
     * @param node:
     * @param json:
     * @return: void
     **/
    public static void iterateNodes(Element node,JSONObject json){
        //获取当前元素的名称
        String nodeName = node.getName();
        //判断已遍历的JSON中是否已经有了该元素的名称
        if(json.containsKey(nodeName)){
            //该元素在同级下有多个
            Object Object = json.get(nodeName);
            JSONArray array = null;
            if(Object instanceof JSONArray){
                array = (JSONArray) Object;
            }else {
                array = new JSONArray();
                array.add(Object);
            }
            //获取该元素下所有子元素
            List<Element> listElement = node.elements();
            if(listElement.isEmpty()){
                //该元素无子元素，获取元素的值
                String nodeValue = node.getTextTrim();
                array.add(nodeValue);
                json.put(nodeName, array);
                return ;
            }
            //有子元素
            JSONObject newJson = new JSONObject();
            //遍历所有子元素
            for(Element e:listElement){
                //递归
                iterateNodes(e,newJson);
            }
            array.add(newJson);
            json.put(nodeName, array);
            return ;
        }
        //该元素同级下第一次遍历
        //获取该元素下所有子元素
        List<Element> listElement = node.elements();
        if(listElement.isEmpty()){
            //该元素无子元素，获取元素的值
            String nodeValue = node.getTextTrim();
            json.put(nodeName, nodeValue);
            return ;
        }
        //有子节点，新建一个JSONObject来存储该节点下子节点的值
        JSONObject object = new JSONObject();
        //遍历所有一级子节点
        for(Element e:listElement){
            //递归
            iterateNodes(e,object);
        }
        json.put(nodeName, object);
        return ;
    }


}
