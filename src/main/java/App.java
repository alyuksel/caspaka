import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class App {

    public static void main(String[] args) {
        Config config = ConfigFactory.parseResources("defaults.conf");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[4]")
                .setAppName("WordCountingApp");
        sparkConf.set("spark.cassandra.connection.host", config.getString("spark.cassandra.connection.host"));

        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf, Durations.seconds(5));

        Map<String, Object> kafkaParams = new HashMap<String, Object>();

        kafkaParams.put("bootstrap.servers", config.getString("kafka.bootstrap.servers"));
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", config.getString("kafka.group.id"));
        kafkaParams.put("auto.offset.reset", config.getString("kafka.auto.offset.reset"));
        kafkaParams.put("enable.auto.commit", config.getBoolean("kafka.enable.auto.commit"));

        Collection<String> topics = Collections.singletonList(config.getString("bootstrap.servers"));

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

        JavaPairDStream<String, String> results = messages
                .mapToPair(
                        record -> new Tuple2<>(record.key(), record.value())
                );

        JavaDStream<String> lines = results
                .map(Tuple2::_2);

        JavaDStream<String> words = lines
                .flatMap(x -> Arrays.asList(x.split("\\s+")).iterator());

        JavaPairDStream<String, Integer> wordCounts = words
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        Map<String, String> map = new HashMap<>();
        map.put("word", "word");
        map.put("count", "count");

        wordCounts.foreachRDD(
                javaRdd -> {
                    Map<String, Integer> wordCountMap = javaRdd.collectAsMap();
                    for (String key : wordCountMap.keySet()) {
                        List<Word> wordList = Arrays.asList(new Word(key, wordCountMap.get(key)));
                        JavaRDD<Word> rdd = streamingContext.sparkContext().parallelize(wordList);
                        javaFunctions(rdd).writerBuilder(
                                "vocabulary", "words", mapToRow(Word.class, map)).saveToCassandra();
                    }
                }
        );

        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
