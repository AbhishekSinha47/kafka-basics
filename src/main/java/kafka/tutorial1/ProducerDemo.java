package kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class ProducerDemo {
    public static void main(String[] args) {
        // 1. create producer properties
        String bootstrapSevers = "127.0.0.1:9092";
        Properties properties = new Properties();

        log.info("We are here!!!");
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

        //3. Create producer record
        ProducerRecord<String,String> record = new ProducerRecord<>("second_topic", "This is hello world!");

        // 4. send data - asynchronous
        // so this executes at background, the program exits and data is never sent. For this we need to flush and close the producer.
        producer.send(record);

        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }

}
