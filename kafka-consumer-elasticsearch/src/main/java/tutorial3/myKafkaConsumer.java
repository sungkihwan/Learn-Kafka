package tutorial3;

public class myKafkaConsumer {
    public static final String BOOT_STRAP_SERVERS = "127.0.0.1:9092";
    public static final String GROUP_ID = "kafka-elasticsearch";
    public static final String TOPIC = "twitter_tweets";
    public static final String AUTO_OFFSET_RESET = "earliest";
}
