package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Dispatcher implements Runnable{

    private static final Logger logger = LogManager.getLogger();
    private String fileLocation;
    private String topicName;
    private KafkaProducer<Integer,String> producer;

    Dispatcher(KafkaProducer<Integer,String> producer,String topicName,String fileLocation){
        this.producer = producer;
        this.fileLocation = fileLocation;
        this.topicName = topicName;

    }

    @Override
    public void run() {

        logger.info("Starting "+fileLocation);

        File file = new File(fileLocation);

        int counter = 0;

        try {
            Scanner sc = new Scanner(file);

            while (sc.hasNext()){
                String line = sc.nextLine();
                producer.send(new ProducerRecord<>(topicName,null,line));
                counter++;
            }
            logger.info("finished");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
