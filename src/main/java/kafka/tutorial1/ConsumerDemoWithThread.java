package kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    public ConsumerDemoWithThread() {

    }

    //since main is static, calling a non-static class(ConsumerThread) from static is not allowed.
    //So declaring the run method outside to invoke the same.
    public void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        // Read balancing consumer groups (Important)
        // When we run multiple consumer groups with same id, the read gets balanced - every consumer gets specific partition assigned
        // Ex - consumer1: second_topic_1  & consumer2 : second_topic_0, second_topic_2
        String groupId = "my-sixth-application";
        String topic = "second_topic";

        // latch for dealing with multiple threads
        // main thread waits for One thread before it starts
        CountDownLatch latch = new CountDownLatch(1);

        //create the consumer thread
        logger.info("Starting consumer thread");
        Runnable consumerThreadObject = new ConsumerThread(
                bootstrapServers,
                topic,
                groupId,
                latch
        );

        //start the thread
        Thread myThread = new Thread(consumerThreadObject);
        myThread.start();

        //add shutdown hook. This would keep listening for any shutdown signal
        //lambda function - Java8
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught Shutdown hook");
            ((ConsumerThread) consumerThreadObject).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                logger.info("Application has exited");
            }
        } ));


        //this makes the main thread to wait until myThread execution is done.
        //latch.countDown() and latch.await() combination is used to make main thread wait for other threads to finish.
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application for interrupted ", e);
        } finally {
            logger.info("Application is closing");
        }


    }


    public class ConsumerThread implements Runnable {
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private String topic;
        private String groupId;
        private String bootstrapServers;

        public ConsumerThread(String bootstrapServers,
                              String topic,
                              String groupId,
                              CountDownLatch latch
                              ) {
            this.latch = latch;
            this.topic = topic;
            this.groupId = groupId;
            this.bootstrapServers = bootstrapServers;
            Properties properties = new Properties();
            //1. Consumer properties
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
            this.consumer = new KafkaConsumer<String, String>(properties);

            //3. Subscribe to topics
            // Can subscribe to single topic - Collections.singleton("second_topic")
            // can subscribe to array of topics - Arrays.asList("first_topic")
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //4. poll to topics
            //poll has deprecated - from kafka 2.0.0
            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key: " + record.key() + ",value: " + record.value());
                        logger.info("Partition: " + record.partition() + ",Offset:" + record.offset());
                    }
                }
            } catch (WakeupException exception) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                //this indicates the main thread that these threads are done with their executions
                // and it can resume its own execution further.
                // as the name suggests it marks end of 'waiting countdown' for main thread.
                latch.countDown();

            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
