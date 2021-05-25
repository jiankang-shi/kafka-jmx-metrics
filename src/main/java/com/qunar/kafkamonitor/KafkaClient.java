package com.qunar.kafkamonitor;

import com.yammer.metrics.reporting.JmxReporter;


import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.*;


public class KafkaClient<S> {
    static String title = "s.ops.kafka.metrics.";
    static long timeMillis = System.currentTimeMillis() / 1000;
    // static String rmiAddress = "l-logcollectkafka1.data.cn2";
    static HashMap<String, Long> mapcl = new HashMap<>();
    static HashMap<String, Long> mapAll = new HashMap<>();
    //    static long tempkey;
    static List<String> list = new ArrayList<>();

    public static void main(String[] args) throws InterruptedException {
        //第一级目录
//        try {
//            JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://l-hdpstest14.data.cn0:9988/jmxrmi");
//            JMXConnector connect = JMXConnectorFactory.connect(jmxServiceURL, null);
//
//            MBeanServerConnection mBeanServerConnection = connect.getMBeanServerConnection();
//
//            String[] domains = mBeanServerConnection.getDomains();
//            for (int i = 0; i < domains.length; i++) {
//                String obj = domains[i];
////                System.out.printf("domian[%d] = %s", i, domains[i].toString());
////                System.out.println();
//            }
        //while (true) {
            connection(args);
          //  sys();

           // Thread.sleep(60000);
        //}
    }

    public static void connection(String[] host) {
        try {
            HashMap<String, Object> prop = new HashMap<String, Object>();
            for (String arg : host) {
                String rmiAddress = arg;
                JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + arg + "/jmxrmi");
                JMXConnector conn = JMXConnectorFactory.connect(url, prop);
                MBeanServerConnection mbsc = conn.getMBeanServerConnection();
                //brokerInformation(mbsc, rmiAddress.split(":")[0]);
                //printAllTopicsInformation(mbsc, rmiAddress.split(":")[0]);

            }
            //printMBeans(mbsc);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sys() throws IOException {

        for (Map.Entry entry : mapcl.entrySet()) {
            Object mapKey = entry.getKey();
            Long mapValue = (Long) entry.getValue();
            String me = mapKey + " " + mapValue + " " + timeMillis;
            WatcherSink url1 = new WatcherSink("qmon-ops.corp.qunar.com", 2013);
            System.out.println(me);
            url1.open();
            url1.invoke(me);
        }
    }


    /**
     * getcount
     **/
    public static void analysisgetCount(String metrics, MBeanServerConnection mbsc, String rmiAddress) throws MalformedObjectNameException, InterruptedException {
        ObjectName mbeanName = new ObjectName(metrics);
        JmxReporter.MeterMBean meterMBean = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.MeterMBean.class);

        String temp = title + rmiAddress.replace(".", "_") + "." + metrics.replace(":", ".").replace("type=", "").replace("name=", "").replace(",", ".");
        if (mapAll.get(temp) != null) {
            long oldvalue = mapAll.get(temp);
            long newvalue = meterMBean.getCount() - oldvalue;
            mapcl.put(temp, newvalue);
        }
        mapAll.put(temp, meterMBean.getCount());
    }


    public static void analysisgetValue(String metrics, MBeanServerConnection mbsc, String rmiAddress) throws MalformedObjectNameException {
        ObjectName mbeanName = new ObjectName(metrics);
        JmxReporter.GaugeMBean meterMBean = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.GaugeMBean.class);
        String temp = title + rmiAddress.replace(".", "_") + "." + metrics.replace(":", ".").replace("type=", "").replace("name=", "").replace(",", ".");
        if (mapAll.get(temp) != null) {
            long oldvalue = mapAll.get(temp);
            long newvalue = Long.parseLong(String.valueOf(meterMBean.getValue()));
            mapcl.put(temp, newvalue);
        }
        mapAll.put(temp, Long.parseLong(String.valueOf(meterMBean.getValue())));
    }



    /**
     * kafka-broker-totalAllTopicsMessagesInPerSec
     **/
    private static void brokerInformation(MBeanServerConnection mbsc, String rmiAddress) throws MalformedObjectNameException, InterruptedException {
        String data[] = {"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec", "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec", "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec", "kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec",
                "kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec", "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions", "kafka.server:type=ReplicaManager,name=LeaderCount", "kafka.server:type=ReplicaManager,name=PartitionCount"
                , "kafka.controller:type=KafkaController,name=OfflinePartitionsCount", "kafka.server:type=ReplicaManager,name=IsrShrinksPerSec", "kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec"};
//        String name = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec";
        for (String name : data) {
            if (name.equals("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec")) {
                analysisgetCount(name, mbsc, rmiAddress);//message
            } else if (name.equals("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec")) {
                analysisgetCount(name, mbsc, rmiAddress);//Out-byte
            } else if (name.equals("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec")) {
                analysisgetCount(name, mbsc, rmiAddress);//In-byte
            } else if (name.equals("kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec")) {
                analysisgetCount(name, mbsc, rmiAddress);//失败拉取请求速率
            } else if (name.equals("kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec")) {
                analysisgetCount(name, mbsc, rmiAddress);//失败发送请求速率
            } else if (name.equals("kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions")) {
                analysisgetValue(name, mbsc, rmiAddress);//正在做复制得partition数量
            } else if (name.equals("kafka.server:type=ReplicaManager,name=LeaderCount")) {
                analysisgetValue(name, mbsc, rmiAddress);//leader副本数
            } else if (name.equals("kafka.server:type=ReplicaManager,name=PartitionCount")) {
                analysisgetValue(name, mbsc, rmiAddress);
            } else if (name.equals("kafka.controller:type=KafkaController,name=OfflinePartitionsCount")) {
                analysisgetValue(name, mbsc, rmiAddress);
            } else if (name.equals("kafka.server:type=ReplicaManager,name=IsrShrinksPerSec")) {
                analysisgetCount(name, mbsc, rmiAddress);
            } else {
                ObjectName mbeanName = new ObjectName(name);
                JmxReporter.MeterMBean meterMBean = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.MeterMBean.class);
                String temp = title + rmiAddress.replace(".", "_") + "." + name.replace(":", ".").replace("type=", "").replace("name=", "").replace(",", ".");
                if (mapAll.get(temp) != null) {
                    long oldvalue = mapAll.get(temp);
                    long newvalue = meterMBean.getCount() - oldvalue;
                    mapcl.put(temp, newvalue);
                }
                mapAll.put(temp, meterMBean.getCount());
            }
        }
    }


    private static void printAllTopicsInformation(MBeanServerConnection mbsc, String rmiAddress) throws MalformedObjectNameException, IOException {
        /**
         * broker/topic一分钟内平均每秒入流量，单位：记录数
         * */
        for (String s : printMBeans(mbsc)) {
            ObjectName mbeanName = new ObjectName(s);
            JmxReporter.MeterMBean meterMBean = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.MeterMBean.class);
            String temp = title + rmiAddress.replace(".", "_") + "." + mbeanName.toString().replace(":", ".").replace("type=", "").replace("name=", "").replace("topic=", "topic.").replaceAll(",", ".");
            if (mapAll.get(temp) != null) {
                long oldvalue = mapAll.get(temp);
                long newvalue = meterMBean.getCount() - oldvalue;
                mapcl.put(temp, newvalue);
            }
            mapAll.put(temp, meterMBean.getCount());


        }


    }




    /**
     * 1.topic-MessagesInPerSec
     **/
    public static List<String> printMBeans(MBeanServerConnection mbsc) throws IOException {
        Set MBeanset = mbsc.queryMBeans(null, null);
        Iterator MBeansetIterator = MBeanset.iterator();
        List<String> list = new ArrayList<String>();
        while (MBeansetIterator.hasNext()) {
            ObjectInstance objectInstance = (ObjectInstance) MBeansetIterator.next();
            //System.out.println(objectInstance.getObjectName());//all metrics
            String topicDatas[] = {"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=", "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=", "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic="};

            for (String topicData : topicDatas)
                if (objectInstance.getObjectName().toString().startsWith(topicData)) {
                    //System.out.println(objectInstance.getObjectName());
                    ObjectName bb = objectInstance.getObjectName();
                    list.add(bb.toString());
                }
        }
        return list;
    }
}


