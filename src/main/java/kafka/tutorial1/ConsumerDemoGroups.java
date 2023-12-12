package kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        // Read balancing consumer groups (Important)
        // When we run multiple consumer groups with same id, the read gets balanced - every consumer gets specific partition assigned
        // Ex - consumer1: second_topic_1  & consumer2 : second_topic_0, second_topic_2
        String groupId = "my-fourth-application";
        String topic = "second_topic";
        //1. Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //possible values for auto-offset-reset are:
        // earliest - reads from start of topic
        // latest - reads the messages which comes now onwards
        // none - throws error.
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        //2. Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);


        //3. Subscribe to topics
        // Can subscribe to single topic - Collections.singleton("second_topic")
        // can subscribe to array of topics - Arrays.asList("first_topic")
        consumer.subscribe(Arrays.asList(topic));


        //4. poll to topics
        //poll has deprecated - from kafka 2.0.0
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records){
                logger.info("key: " + record.key() + ",value: " + record.value());
                logger.info("Partition: " + record.partition() + ",Offset:" + record.offset());
            }
        }





    }
}
