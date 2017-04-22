import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

/**
 * Created by Alec on 4/21/2017.
 * main for term project
 */
public class Main {
	public static void main(String[] args){

		SparkConf conf = new SparkConf().setAppName("AuctionBid").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> rawData = sc.textFile("hdfs://denver:30321/455TP/data/daily");
		JavaRDD<String> stations = sc.textFile("hdfs://denver:30321/455TP/ghcnd-stations.txt");

		// get ids of stations in colorado
		Object[] coStations;
		coStations = stations.filter(
				(Function<String, Boolean>) line -> line.substring(38,40).trim().equals("CO")
		).map(
				(Function<String, String>) line -> line.substring(0,11)
		).collect().toArray();

		// get just data from stations in colorado
		JavaRDD<String> coData = rawData.filter(
				(Function<String, Boolean>) line -> Arrays.asList(coStations).contains(line.substring(0,11))
		).persist(StorageLevel.MEMORY_ONLY());

		System.out.println("lines: " + coData.count());

		coData.coalesce(1,true).saveAsTextFile("hdfs://denver:30321/455TP/out");

		coData.unpersist();

		sc.stop();

	}
}
