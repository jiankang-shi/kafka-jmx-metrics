package com.qunar.kafkamonitor;

import java.io.IOException;
import java.util.*;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.yammer.metrics.reporting.JmxReporter;
import sun.rmi.runtime.Log;

/**
 * 通过rmi来调用kafka里的jmx信息
 *
 * @author root
 */
public class JMX1<S> {
    static String title = "s.data.pf_kafka_docker_log.";
    static long timeMillis = System.currentTimeMillis() / 1000;
    static HashMap<String, Long> mapcl = new HashMap<>();
    static HashMap<String, Long> mapAll = new HashMap<>();
    static List<String> list = new ArrayList<>();


    static final String[] oriMetricKey = {"kafka.log:type=Log,name=LogEndOffset","kafka.log:type=Log,name=Size", "kafka.cluster:type=Partition,name=UnderReplicated","kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions"
            ,"kafka.server:type=ReplicaManager,name=LeaderCount", "kafka.server:type=ReplicaManager,name=PartitionCount", "kafka.controller:type=KafkaController,name=OfflinePartitionsCount"
            ,"kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec","kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent","kafka.network:type=Processor,name=IdlePercent","kafka.network:type=RequestChannel,name=RequestQueueSize","kafka.network:type=RequestChannel,name=ResponseQueueSize"
            ,"kafka.server:type=KafkaServer,name=BrokerState","kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec",};
    static final String[] oriMetricKey2 = {"kafka.server:type=ReplicaManager,name=IsrExpandsPerSec","kafka.server:type=ReplicaManager,name=IsrShrinksPerSec","kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec","kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec","kafka.server:type=BrokerTopicMetrics,name=TotalFetchRequestsPerSec","kafka.server:type=BrokerTopicMetrics,name=TotalProduceRequestsPerSec"
            ,"kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec","kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec","kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce","kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs"};
    static final String[] oriMetricKey3 = {};

    public static void main(String[] args) {
        try {
            HashMap<String, Object> prop = new HashMap<String, Object>();
            for (String arg : args) {
                JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + arg + "/jmxrmi");
                JMXConnector conn = JMXConnectorFactory.connect(url, prop);
                MBeanServerConnection mbsc = conn.getMBeanServerConnection();

                while (true){
                    printMBeans(mbsc,arg);
                    sys(arg);
                    Thread.sleep(60000);
                }


            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sys(String host) throws Exception {
        WatcherSink url1 = new WatcherSink("qmon-ops.corp.qunar.com", 2013);
        url1.open();
        timeMillis = System.currentTimeMillis() / 1000;


        for (Map.Entry entry : mapcl.entrySet()) {
            Object mapKey = entry.getKey();
            Long mapValue = (Long) entry.getValue();
            String me = mapKey+".value" + " " + mapValue + " " +  timeMillis;
            System.out.println(me);
            url1.invoke(me);


        }
        for (String list1 : list) {
            String me = title+host.split(":")[0].replace(".","_")+"."+ list1 + " " +  timeMillis;
            System.out.println(me);
            url1.invoke(me);

        }
        list.clear();
        //url1.close();
    }


    private static void printAllTopicsBytesOutPerSec(MBeanServerConnection mbsc) throws MalformedObjectNameException {
        String name = "kafka.log:type=Log,name=LogEndOffset,topic=";

        ObjectName mbeanName = new ObjectName(name);

        JmxReporter.GaugeMBean meterMBean = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.GaugeMBean.class);
        System.out.println(name.replace(":", ".").replace("type=", "").replace("topic=", "topic.").replace("name=", "").replace(",", ".").replace("=", ".") + " " + meterMBean.getValue());
    }

    private static void printAllTopicsBytesInPerSec(MBeanServerConnection mbsc) throws MalformedObjectNameException {
        String name = "\"kafka.server\":type=\"BrokerTopicMetrics\",name=\"AllTopicsBytesInPerSec\"";
        ObjectName mbeanName = new ObjectName(name);

        JmxReporter.MeterMBean meterMBean = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.MeterMBean.class);
        System.out.println(meterMBean.getCount() + "," + meterMBean.getEventType() + "," + meterMBean.getFifteenMinuteRate()
                + meterMBean.getFiveMinuteRate() + "," + meterMBean.getMeanRate() + "," + meterMBean.getOneMinuteRate() +
                "," + meterMBean.getRateUnit());
    }

    /**
     * kafka-logSize
     **/
    public static void formatLogName(MBeanServerConnection mbsc, String Logname) throws MalformedObjectNameException {
        ObjectName mbeanName = new ObjectName(Logname);
        if (Logname.equals("kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec")
                || Logname.equals("kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec") || Logname.equals("kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec")
                || Logname.equals("kafka.server:type=ReplicaManager,name=IsrShrinksPerSec")||Logname.equals("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec")
                ||Logname.equals("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec")||Logname.equals("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec")||Logname.equals("kafka.server:type=ReplicaManager,name=IsrExpandsPerSec")) {
            JmxReporter.MeterMBean meterMBean = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.MeterMBean.class);
            String items = Logname.replace(":", ".").replace("type=", "").replace("name=", ".").replace("topic=", ".").replace(",.", ".").replace(",", ".").replace("=", ".")+".value" + " " + meterMBean.getCount();
            list.add(items);
        } else {
            JmxReporter.GaugeMBean meterMBean = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.GaugeMBean.class);
            String items = Logname.replace(":", ".").replace("type=", "").replace("name=", ".").replace("topic=", ".").replace(",.", ".").replace(",", ".").replace("=", ".")+".value" + " " + meterMBean.getValue();
            list.add(items);
        }

    }

    /**
     * 1.topic-MessagesInPerSec
     * 2.topic-BytesInPerSec/BytesInPerSec
     **/
    public static void topicPerSec(MBeanServerConnection mbsc, String topic_list,String host) throws MalformedObjectNameException {
        ObjectName mbeanName = new ObjectName(topic_list);
        JmxReporter.MeterMBean meterMBean = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.MeterMBean.class);
        String items = title +host.split(":")[0].replace(".","_")+"." + topic_list.replace(":", ".").replace("type=", "").replace("name=", ".").replace("topic=", ".").replace(",", "");
        if (mapAll.get(items) != null) {
            long oldvalue = mapAll.get(items);
            long newvalue = meterMBean.getCount() - oldvalue;
            mapcl.put(items, newvalue);
        }
        mapAll.put(items, meterMBean.getCount());
    }


    /**
     * 获取所有的mxbean 过滤
     *
     * @param mbsc
     * @throws IOException
     */
    private static void printMBeans(MBeanServerConnection mbsc,String host) throws IOException, MalformedObjectNameException {
        Set MBeanset = mbsc.queryMBeans(null, null);
        //System.out.println("MBeanset.size() : " + MBeanset.size());
        Iterator MBeansetIterator = MBeanset.iterator();
        while (MBeansetIterator.hasNext()) {
            ObjectInstance objectInstance = (ObjectInstance) MBeansetIterator.next();
            for (String name : oriMetricKey) {
                if (objectInstance.getObjectName().toString().startsWith(name)) {
                    formatLogName(mbsc, objectInstance.getObjectName().toString());
                }
            }
            for (String name1 : oriMetricKey2) {
                if (objectInstance.getObjectName().toString().startsWith(name1)) {
                    topicPerSec(mbsc, objectInstance.getObjectName().toString(),host);
                }
            }

        }


    }
}