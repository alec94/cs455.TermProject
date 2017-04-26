import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 * Created by Justin on 4/23/2017.
 */
public class Snowfall {

	public static JavaRDD<String> filterSnowfall(JavaRDD<Summary> coData) {

		// get total monthly snowfall
		JavaRDD<String> coSnowfall = coData.filter(
				(Function<Summary, Boolean>) line -> line.getElement().equals("SNOW")
		).map(
				(Function<Summary, String>) line -> {
					String year = String.valueOf(line.getYear());
					String month = line.getMonth();

					int snowfall = 0;
					int[] values = line.getValues();
					char[] qFlags = line.getQFlags();

					for (int i = 0; i < values.length; i++) {
						// ensure values are valid
						if ((qFlags[i] == ' ' || qFlags[i] == Character.MIN_VALUE) && values[i] > 0) {
							snowfall += values[i];
						}
					}

					return year + "," + snowfall;
				}
		).persist(StorageLevel.MEMORY_ONLY());

		//  aggregate month values
		JavaPairRDD<String, Tuple2<Integer, Integer>> pairRDD = coSnowfall.mapToPair(s -> {
			String[] tokens = s.split(",");
			return new Tuple2(tokens[0], new Tuple2(Integer.parseInt(tokens[1]), 1));
		});

		JavaPairRDD<String, Tuple2<Integer, Integer>> snowAverages = pairRDD.reduceByKey(
				(Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>)
						(acc, val) -> new Tuple2(acc._1 + val._1, acc._2 + val._2)
		).coalesce(1, true).sortByKey();

		/*
		JavaPairRDD<String, Integer> finalStuff = snowAverages.mapToPair(
				(PairFunction<String, Tuple2<Integer, Integer>, Tuple2<String, Integer>>) (s, t) -> {
					int sum = s;
					return new Tuple2(s, t._1 / t._2)
				}
		);
		*/

		JavaRDD<String> yearlySnowfall = snowAverages.map(
				(Function<Tuple2<String, Tuple2<Integer, Integer>>, String>) s -> {
					String year = s._1;
					Integer sum = s._2._1;
					Integer count = s._2._2;
					return year + "," + (sum / count);
				}
		).persist(StorageLevel.MEMORY_ONLY());

		return yearlySnowfall;
	}

}
