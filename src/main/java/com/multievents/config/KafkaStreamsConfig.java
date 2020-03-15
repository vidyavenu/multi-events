package com.multievents.config;

import com.multievents.ParkingAddress;
import com.multievents.ParkingGeolocation;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrapServer;

    @Bean
    public KStream<String, String> parkingKstream(@Value("${spring.application.name}") String applicationName) {
        Properties streamConfigurations = new Properties();

        streamConfigurations.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        streamConfigurations.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName);
        streamConfigurations.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamConfigurations.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        streamConfigurations.put(StreamsConfig.STATE_DIR_CONFIG, "data");
        streamConfigurations.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        streamConfigurations.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        streamConfigurations.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        streamConfigurations.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");
        streamConfigurations.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");
        streamConfigurations.put("value.subject.name.strategy", TopicRecordNameStrategy.class.getName());

        Serializer serializer = new KafkaAvroSerializer();
        serializer.configure(streamConfigurations, false);
        Deserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(streamConfigurations, false);
        Serde<ParkingAddress> parkingAdressSerde = Serdes.serdeFrom(serializer, deserializer);
        Serde<ParkingGeolocation> parkingGeolocationSerde = Serdes.serdeFrom(serializer, deserializer);

        StreamsBuilder parkingDatastreamsBuilder = new StreamsBuilder();
        final KStream<String, String> parkingDataStream = parkingDatastreamsBuilder.stream("parking-lots",
                Consumed.with(Serdes.String(), Serdes.String()));

        parkingDataStream.map((key, value) -> processAddressDataStream(value))
                .peek((k, v) -> System.out.println(k + " => " + v))
                .to("parking-location", Produced.with(Serdes.String(), parkingAdressSerde));

        parkingDataStream.map((key, value) -> processGeolocationDataStream(value))
                .peek((k, v) -> System.out.println(k + " => " + v))
                .to("parking-location", Produced.with(Serdes.String(), parkingGeolocationSerde));

        final KafkaStreams streams = new KafkaStreams(parkingDatastreamsBuilder.build(), streamConfigurations);

        final CountDownLatch startLatch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
                startLatch.countDown();
            }

        });
        streams.start();
        try {
            if (!startLatch.await(60, TimeUnit.SECONDS)) {
                throw new RuntimeException("Streams never finished rebalancing on startup");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return parkingDataStream;
    }

    private KeyValue<String, ParkingAddress> processAddressDataStream(String parkingStreamValue) {
        JSONParser parser = new JSONParser();
        ParkingAddress parkingAddress = new ParkingAddress();
        String parkingKey = "";
        try {
            JSONObject parkingJsonObject = (JSONObject) parser.parse(parkingStreamValue);

            parkingKey = parkingJsonObject.get("st_marker_id").toString() + parkingJsonObject.get("st_marker_id").toString();
            String parkingStatus = parkingJsonObject.get("status").toString();

            JSONObject locationObj = (JSONObject) parkingJsonObject.get("location");
            String human_address = locationObj.get("human_address").toString();

            parkingAddress.setHumanAddress(human_address);
            parkingAddress.setStatus(parkingStatus);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new KeyValue<>(parkingKey, parkingAddress);
    }


    private KeyValue<String, ParkingGeolocation> processGeolocationDataStream(String parkingStreamValue) {
        JSONParser parser = new JSONParser();
        ParkingGeolocation parkingGeolocation = new ParkingGeolocation();
        String parkingKey = "";
        try {
            JSONObject parkingJsonObject = (JSONObject) parser.parse(parkingStreamValue);
            parkingKey = parkingJsonObject.get("st_marker_id").toString() + parkingJsonObject.get("st_marker_id").toString();
            String parkingStatus = parkingJsonObject.get("status").toString();

            Double parkingLatitude = Double.parseDouble(parkingJsonObject.get("lat").toString());
            Double parkingLongitude = Double.parseDouble(parkingJsonObject.get("lon").toString());

            parkingGeolocation.setLatitude(parkingLatitude);
            parkingGeolocation.setLongitude(parkingLongitude);
            parkingGeolocation.setStatus(parkingStatus);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new KeyValue<>(parkingKey, parkingGeolocation);
    }
}




