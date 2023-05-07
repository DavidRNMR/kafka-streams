package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        logger.info("first transaction...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,AppConfigs.applicationID);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();

        logger.info("Start sending messages...");
        producer.beginTransaction();

        for (int i = 1; i <= AppConfigs.numEvents; i++) {
            producer.send(new ProducerRecord<>(AppConfigs.topicName, i, "Simple Message-1" + i));
            producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Simple Message-2" + i));
        }

        producer.commitTransaction();

        logger.info("Finished - Closing Kafka Producer.");
        producer.close();


        logger.info("Segunda transaction...");
        producer.beginTransaction();

        for (int i = 1; i <= AppConfigs.numEvents; i++) {
            producer.send(new ProducerRecord<>(AppConfigs.topicName, i, "Simple Message-1" + i));
            producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Simple Message-2" + i));
        }

        producer.commitTransaction();

        logger.info("Finished - Closing Kafka Producer.");
        producer.close();

    }
}
