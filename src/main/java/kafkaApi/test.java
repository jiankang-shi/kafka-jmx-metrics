package kafkaApi;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.utils.Time;


public class test {
    private static AdminClient adminClient = null;
    public static AdminClient client;


    public static void connectionKafka() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "l-logcollectkafka1.ops.cna:9092");
        props.put("request.timeout.ms", 600000);
        adminClient = AdminClient.create(props);
//        try {
//            addPartitions("test1", 25);
//        } catch (Exception exception) {
//            exception.printStackTrace();
//        }
    }

    public static void addPartitions(String topic, Integer numPartitions) throws Exception {
        NewPartitions newPartitions = NewPartitions.increaseTo(numPartitions);
        Map<String, NewPartitions> map = new HashMap<>(1, 1);
        map.put(topic, newPartitions);
        adminClient.createPartitions(map).all().get(3, TimeUnit.SECONDS);
    }
    public static boolean topicExists(String topicName) {
        // 注意下面几个参数的值适当大一些,否则可能会出现连接异常
        // sessiontimeout in milliseconds
        // connectionTimeoutMs connection timeout in milliseconds
        // maxInFlightRequests
        KafkaZkClient kafkaClient = KafkaZkClient.apply("l-logcollectkafka1.ops.cna:2181", false, 20000, 20000, 40000, Time.SYSTEM, "", "",null,null);
        return kafkaClient.topicExists(topicName);
    }



        public static void main (String[]args){
        connectionKafka();

        }

    }
