package com.sikku.kafka.streamsdemo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class WordCountStream {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-dataflow");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		StreamsBuilder builder = new StreamsBuilder();
		KStream<Object, Object> stream = builder.stream("streams-wordcount-input");

		KTable<String, Long> countsTable = stream
				.flatMapValues(value -> Arrays.asList(value.toString().toLowerCase().split(" ")))
				.groupBy((key, value) -> value).count();

		countsTable.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

		Topology topology = builder.build();
		System.out.println(topology.describe());

		KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}

}
