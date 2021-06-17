package apps.smartfwd.src.main.java.utils;


import java.util.Properties;


public class Producer {

//    public static final KafkaProducer<String,String> producer;

    /*
    初始化生产者
     */
    static {
        Properties configs = initConfig();
//        producer = new KafkaProducer<String, String>(configs);
    }

    /*
    初始化配置
     */
    private static Properties initConfig(){
        Properties props = new Properties();
        props.put("bootstrap.servers", MQDict.MQ_ADDRESS_COLLECTION);
//        props.put("acks", "0");
        props.put("retries", 0);
        props.put("batch.size", 16384);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }


    public static void main(String[] args) throws InterruptedException {
        //消息实体
//        ProducerRecord<String , String> record = null;
//        for (int i = 0; i < 100; i++) {
//            record = new ProducerRecord<String, String>(MQDict.PRODUCER_TOPICA, "value"+i);
//            //发送消息
//            producer.send(record, new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    if (null != e){
//                        e.printStackTrace();
//                    }else {
//                        System.out.println(String.format("offset:%s,partition:%s",recordMetadata.offset(),recordMetadata.partition()));
//                    }
//                }
//            });
//        }
//        producer.close();
    }
}