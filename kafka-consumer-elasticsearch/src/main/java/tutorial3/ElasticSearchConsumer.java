package tutorial3;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.InfoRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static tutorial3.myKafkaConsumer.*;

public class ElasticSearchConsumer {

    public static RestClient restClient() {

//        https://79pmruidg0:k8dzxg2i59@kafka-consumer-elast-8691160372.us-east-1.bonsaisearch.net:443
        String hostname = "kafka-consumer-elast-8691160372.us-east-1.bonsaisearch.net";
        String username = "79pmruidg0";
        String password = "k8dzxg2i59";

        // basic auth
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                        new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestClient restClient = builder.build();

        return restClient;
    }

    public static void main(String[] args) throws IOException {
        RestClient restClient = restClient();

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        KafkaConsumer<String, String> consumer = createConsumer();

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            //TODO: 아직 연결 덜했다. record.value 가져온거 elastic search에 넣어야함.
            for (ConsumerRecord<String, String> record : records) {
//                logger.info("Key : " + record.key() + ", Value : " + record.value());
//                logger.info("Partition : " + record.partition() + ", Offset : " + record.offset());

                String jsonString = record.value();

                Request request = new Request("PUT", "/twitter/tweets/");
                //let's assume that the documents are stored in an HttpEntity array

                Response response = restClient.performRequest(request);
                RequestLine requestLine = response.getRequestLine();
                String responseBody = EntityUtils.toString(response.getEntity());

                logger.info("requestLine = " + requestLine);
                logger.info("responseBody = " + responseBody);

            }
        }
    }

    public static KafkaConsumer<String, String> createConsumer() {
        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOT_STRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET); // latest

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(TOPIC));
        return consumer;
    }
}
