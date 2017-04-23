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

		System.out.println("Starting...");
		SparkConf conf = new SparkConf().setAppName("cs455 Term Project");
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
		JavaRDD<Summary> coData = rawData.filter(
				(Function<String, Boolean>) line -> Arrays.asList(coStations).contains(line.substring(0,11)) && !line.substring(11,15).trim().equals("-9999")
		).map((Function<String, Summary>) line -> {
			Summary summary = new Summary();
			summary.setID(line.substring(0,11));
			summary.setYear(Integer.parseInt(line.substring(11,15).trim()));
			summary.setMonth(line.substring(15,17));
			summary.setElement(line.substring(17,21));
			int startV = 21;
			int startM = 26;
			int startQ = 27;
			int startS = 28;

			for (int i = 1; i <= 31; i++){
				summary.setValue(i,Integer.parseInt(line.substring(startV,startV + 5).trim()));
				summary.setMFlag(i,line.charAt(startM));
				summary.setQFlag(i, line.charAt(startQ));
				summary.setSFlag(i, line.charAt(startS));

				startV += 8;
				startM += 8;
				startQ += 8;
				startS += 8;
			}

			return summary;
		}).persist(StorageLevel.MEMORY_ONLY());

		// get total monthly snowfall
		JavaRDD<String> coSnowfall = coData.filter(
				(Function<Summary, Boolean>) line -> line.getElement().equals("SNOW")
		).map(
				(Function<Summary, String>) line -> {
					String id = line.getID();
					String date = line.getYear() + "/" + line.getMonth();
					int snowfall = line.getTotalSnowfall();

					return id + " " + date + ": " + snowfall;
				}
		);

		System.out.println("coSnowfall lines: " + coSnowfall.count());

		coSnowfall.coalesce(1,true).saveAsTextFile("hdfs://denver:30321/455TP/snow-out/");

		coSnowfall.unpersist();

		coData.unpersist();

		sc.stop();

	}
}
