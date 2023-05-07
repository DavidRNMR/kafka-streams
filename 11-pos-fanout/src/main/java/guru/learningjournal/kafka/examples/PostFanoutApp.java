package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.AppSerdes;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class PostFanoutApp {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,AppConfigs.applicationID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);


        //topology
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PosInvoice> KS0 = builder.stream(AppConfigs.posTopicName,
                Consumed.with(AppSerdes.String(),AppSerdes.PosInvoice()));

            KS0.filter((k,v)->
            v.getDeliveryType().equals(AppConfigs.DELIVERY_TYPE_HOME_DELIVERY))
                    .to(AppConfigs.shipmentTopicName, Produced.with(AppSerdes.String(),
                            AppSerdes.PosInvoice()));


            KS0.filter((k,v)->
                    v.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME))
                    .mapValues(invoice-> RecordBuilder.getNotification(invoice))
                    .to(AppConfigs.notificationTopic,Produced.with(AppSerdes.String(),
                            AppSerdes.Notification()));


            KS0.mapValues(invoice->RecordBuilder.getMaskedInvoice(invoice))
                    .flatMapValues(invoice-> RecordBuilder.getHadoopRecords(invoice))
                    .to(AppConfigs.hadoopTopic,Produced.with(AppSerdes.String(),
                            AppSerdes.HadoopRecord()));

        KafkaStreams streams = new KafkaStreams(builder.build(),properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            streams.close();
        }));

    }
}
