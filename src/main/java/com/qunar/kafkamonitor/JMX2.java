package com.qunar.kafkamonitor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
//testssssxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.yammer.metrics.reporting.JmxReporter;

/**
 * 通过rmi来调用kafka里的jmx信息
 * @author root
 *
 */
public class JMX2<S> {
    static String title = "s.ops.kafka.metrics.";
    static long timeMillis = System.currentTimeMillis() / 1000;
    static String removeStr = "kafka.server.";
    // static String rmiAddress = "l-logcollectkafka1.data.cn2";
    static HashMap<String, Long> mapcl = new HashMap<>();
    static HashMap<String, Long> mapAll = new HashMap<>();

    public static void main(String[] args) {
        try {
            String rmiAddress = "l-logcollectkafka1.ops.cna:9988";
            HashMap<String, Object> prop = new HashMap<String, Object>();
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + rmiAddress + "/jmxrmi");
            JMXConnector conn = JMXConnectorFactory.connect(url, prop);
            MBeanServerConnection mbsc = conn.getMBeanServerConnection();


            printAllTopicsBytesInPerSec(mbsc);
//
           //printAllTopicsBytesOutPerSec(mbsc);

            printMBeans(mbsc);

        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }


    private static void printAllTopicsBytesOutPerSec(MBeanServerConnection mbsc) throws MalformedObjectNameException
    {
        String name = "kafka.log:type=Log,name=LogEndOffset,topic=";

        ObjectName mbeanName = new ObjectName(name);

        JmxReporter.GaugeMBean meterMBean = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.GaugeMBean.class);
        System.out.println( name.replace(":", ".").replace("type=", "").replace("topic=","topic.").replace("name=", "").replace(",", ".").replace("=",".")+" "+meterMBean.getValue());
    }

    private static void printAllTopicsBytesInPerSec(MBeanServerConnection mbsc) throws MalformedObjectNameException
    {
        String name = "kafka.server:type=ReplicaManager,name=IsrExpandsPerSec";
        ObjectName mbeanName = new ObjectName(name);

        JmxReporter.MeterMBean meterMBean = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.MeterMBean.class);
        System.out.println(meterMBean.getCount() + "," + meterMBean.getEventType() + "," + meterMBean.getFifteenMinuteRate()
                + meterMBean.getFiveMinuteRate() + "," + meterMBean.getMeanRate() + "," + meterMBean.getOneMinuteRate() +
                "," + meterMBean.getRateUnit());
    }

    /**
     * metrics get value
     *
     * @return**/
    public static String formatLogName(MBeanServerConnection mbsc  , String Logname ) throws MalformedObjectNameException {
        ObjectName mbeanName = new ObjectName(Logname);
        JmxReporter.GaugeMBean meterMBean = JMX.newMBeanProxy(mbsc, mbeanName, JmxReporter.GaugeMBean.class);
        String items =Logname.replace(":",".").replace("type=","").replace("name=",".").replace("topic=",".").replace(",.",".").replace(",",".").replace("=",".")+" "+meterMBean.getValue();


        return items;
    }

    /**
     * 打印所有的mxbean
     * @param mbsc
     * @throws IOException
     */
    private static void printMBeans(MBeanServerConnection mbsc) throws IOException, MalformedObjectNameException {
        Set MBeanset = mbsc.queryMBeans(null, null);
        System.out.println("MBeanset.size() : " + MBeanset.size());
        Iterator MBeansetIterator = MBeanset.iterator();
        while (MBeansetIterator.hasNext()) {
            ObjectInstance objectInstance = (ObjectInstance) MBeansetIterator.next();
            String tm = "kafka.server:type=ReplicaManager,name=IsrExpandsPerSec";
                if ( objectInstance.getObjectName().toString().startsWith(tm)){
                    System.out.println(objectInstance.getObjectName());
                }


            }
        }
    }




