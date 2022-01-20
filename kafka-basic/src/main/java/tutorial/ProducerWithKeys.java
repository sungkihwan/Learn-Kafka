package tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class.getName());
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0;i<10;i++) {

            String topic = "first";
            String value = "hello world" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            // create Producer record
            ProducerRecord<String,String> record =
                    new ProducerRecord<String,String>(topic, key, value);

            logger.info("Key : " + key);

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
            }).get(); // block the .send() to make it synchronous - don't do this in production
        }

        producer.flush();
        producer.close();
    }
}
