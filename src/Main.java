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
	private static String dataPath;
	private static String stationsPath;
	private static JavaRDD<Summary> initData(JavaSparkContext sparkContext){
		JavaRDD<String> rawData = sparkContext.textFile(dataPath);
		JavaRDD<String> stations = sparkContext.textFile(stationsPath);

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

		return coData;
	}
	public static void main(String[] args) {

		if (args.length < 4) {
			System.out.println("ERROR: not enough arguments\nUSAGE: <climate data> <stations data> <output> <element>");
			System.exit(-1);
		}

		dataPath = args[0].trim();
		stationsPath = args[1].trim();

		SparkConf conf = new SparkConf().setAppName("cs455 Term Project");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Summary> coData = initData(sc);
		String element = args[3];

		if (element.toLowerCase().equals("snow")) {
			// get total monthly snowfall
			JavaRDD<String> coSnowfall = Snowfall.filterSnowfall(coData);

			System.out.println("coSnowfall lines: " + coSnowfall.count());
			coSnowfall.coalesce(1, true).saveAsTextFile(args[2] + "/snow");
			coSnowfall.unpersist();

		} else if (element.toLowerCase().equals("tmin")) {
			// get monthly average of min temp
			JavaRDD<String> coTemp = Temperature.filterMinTemperature(coData);

			System.out.println("coTemp lines: " + coTemp.count());
			coTemp.coalesce(1, true).saveAsTextFile(args[2] + "/tmin");
			coTemp.unpersist();

		} else if (element.toLowerCase().equals("tmax")) {
			// get monthly average of max temp
			JavaRDD<String> coTemp = Temperature.filterMaxTemperature(coData);

			System.out.println("coTemp lines: " + coTemp.count());
			coTemp.coalesce(1, true).saveAsTextFile(args[2] + "/tmax");
			coTemp.unpersist();

		} else {
			System.out.println("Unknown element: " + element);
		}

		coData.unpersist();

		sc.stop();

	}
}
