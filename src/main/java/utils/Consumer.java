package apps.smartfwd.src.main.java.utils;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.util.Arrays;
import java.util.Properties;


public class Consumer {

    public static final KafkaConsumer<String,String> consumer;

    /**
     *  初始化消费者s
     */
    static {
        Properties configs = initConfig();
        consumer = new KafkaConsumer<String, String>(configs);
        consumer.subscribe(Arrays.asList(MQDict.CONSUMER_TOPICB));
    }
    /**
     *  初始化配置
     */
    private static Properties initConfig(){
        Properties props = new Properties();
        props.put("bootstrap.servers", MQDict.MQ_ADDRESS_COLLECTION);
        props.put("group.id", MQDict.CONSUMER_GROUP_ID);
        props.put("enable.auto.commit", MQDict.CONSUMER_ENABLE_AUTO_COMMIT);
        props.put("auto.commit.interval.ms", MQDict.CONSUMER_AUTO_COMMIT_INTERVAL_MS);
        props.put("session.timeout.ms", MQDict.CONSUMER_SESSION_TIMEOUT_MS);
        props.put("max.poll.records", MQDict.CONSUMER_MAX_POLL_RECORDS);
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return props;
    }

    public static void main(String[] args) {
        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll();
//            records.forEach((ConsumerRecord<String, String> record)->{
//            });
        }
    }

}
