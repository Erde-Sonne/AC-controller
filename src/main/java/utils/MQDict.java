package apps.smartfwd.src.main.java.utils;


public class MQDict {
    public static final String MQ_ADDRESS_COLLECTION = "192.168.1.49:9092";			//kafka地址
    public static final String PRODUCER_TOPICA = "topicA";						//生产者连接的topic
    public static final String CONSUMER_TOPICB = "topicB";						//消费者连接的topic
    public static final String PRODUCER_TOPICC = "topicC";						//生产者连接的topic
    public static final String PRODUCER_TOPICD = "topicD";						//生产者连接的topic
    public static final String CONSUMER_GROUP_ID = "1";								//groupId，可以分开配置
    public static final String CONSUMER_ENABLE_AUTO_COMMIT = "true";				//是否自动提交（消费者）
    public static final String CONSUMER_AUTO_COMMIT_INTERVAL_MS = "1000";
    public static final String CONSUMER_SESSION_TIMEOUT_MS = "30000";				//连接超时时间
    public static final int CONSUMER_MAX_POLL_RECORDS = 10;							//每次拉取数
    public static final int CONSUMER_POLL_TIME_OUT = 3000;	//拉去数据超时时间

}