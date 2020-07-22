import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class SparkKafkaConsumer {

	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("WordCountingApp");
		
		JavaStreamingContext streamingContext 
			= new JavaStreamingContext(sparkConf, Durations.seconds(10));
		streamingContext.checkpoint("./checkpoint");

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		//kafkaParams.put("auto.offset.reset", "latest");
		//kafkaParams.put("enable.auto.commit", false);
		Collection<String> topics = Arrays.asList("test");

		JavaInputDStream<ConsumerRecord<String, String>> messages
			= KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		JavaPairDStream<String, String> results = messages
				.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
		
		JavaDStream<String> lines = results.map(tuple2 -> tuple2._2());
		
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split("\\s+")).iterator());
		
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
				.reduceByKey((i1, i2) -> i1 + i2);
	
		wordCounts.print();
		
		streamingContext.start();
		streamingContext.awaitTermination();

	}

}
