package kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//@Slf4j
public class ProducerDemoKeys {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // 1. create producer properties
        String bootstrapSevers = "127.0.0.1:9092";
        Properties properties = new Properties();

        /*
         //Older way of setting up producer config
         properties.setProperty("bootstrap.servers", bootstrapSevers);
         //kafka send all data as binary 0 and 1. So if we are sending string, we need to mention stringSerializer
         //as this serializer would convert string to binary format.
         properties.setProperty("key.serializer", StringSerializer.class.getName());
         properties.setProperty("value.serializer", StringSerializer.class.getName());
        */

        //New way of adding producer config
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapSevers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //2.create the producer
        //producing key and value as strings
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {

            String topic = "second_topic";
            String value = "This is message" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            //3. Create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // 4. send data - asynchronous
            // so this executes at background, the program exits and data is never sent. For this we need to flush and close the producer.
            //callback is invoked when the data is sent
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    //executes everytime record is successfully sent or exception is thrown.
                    if (exception == null) {
                        //the record was successfully sent
                        //verified that : the same key always goes to the same partition, for a fixed number of partitions(meaning you dont delete/add new partitions)
                        logger.info("Key value is:: {}",key);
                        logger.info("Received new metadata - Details: Topic = {}, Partition = {}, Offset = {}, TimeStamp = {}",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing: {}", exception);
                    }
                }
            });
        }

        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }

}
