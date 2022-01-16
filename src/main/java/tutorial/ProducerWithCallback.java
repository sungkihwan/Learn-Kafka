package tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class.getName());
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create Producer record
        ProducerRecord<String,String> record =
                new ProducerRecord<String,String>("first", "hello world");

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    //success
                    logger.info("Recieived new metadata. \n" +
                            "Topic : " + metadata.topic() + "\n" +
                            "Partition : " + metadata.partition() + "\n" +
                            "Offset : " + metadata.offset() + "\n" +
                            "Timestamp : " + metadata.timestamp());
                } else {
                    logger.error("Error", exception);
                }
            }
        });
        producer.flush();
        producer.close();
    }
}
