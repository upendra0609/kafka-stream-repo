package com.sikku.kafka.streamsdemo;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class DataFlowStream {

	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-dataflow");
		props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<Object, Object> stream = builder.stream("streams-dataflow-input");
		stream.foreach((key, value) -> System.out.println(key + "  " + value));
		stream.filter((key, value) -> value.toString().contains("token"))
//				.mapValues(value -> value.toString().toUpperCase())
//		.map((key,value)->new KeyValue<>(key,value.toString().toUpperCase()))
		.map((key,value)->KeyValue.pair(value,key))
		
				.to("streams-dataflow-output");

		Topology topology = builder.build();
		System.out.println(topology.describe());

		KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}

}
