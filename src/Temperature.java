import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 * Created by Justin on 4/23/2017.
 */
public class Temperature {

	// this filters the daily average temperature measures from scoped data
	public static JavaRDD<String> filterTemperature (JavaRDD<Summary> coData, String requestedElement) {

		// sanitize inputs, even though only tavg is used
		if(!(requestedElement.equals("TAVG") || requestedElement.equals("TMIN") || requestedElement.equals("TMAX"))) {
			System.err.println("Improper element requested; expected TAVG, TMIN, or TMAX");
			System.exit(-1);
		}

		// get average monthly average temperature (yes i meant to say average twice)
		JavaRDD<String> coTemp = coData.filter(
				(Function<Summary, Boolean>) line -> line.getElement().equals(requestedElement)
		).map(
				(Function<Summary, String>) line -> {

					String year = String.valueOf(line.getYear());
					int total = 0, num = 0;
					int[] values = line.getValues();
					char[] qFlags = line.getQFlags();

					// accumulates and tallies entries from individual stations and coalesces them into months
					for (int i = 0; i < values.length; i++) {
						// ensure respective quality flags pass
						if ((qFlags[i] == ' ' || qFlags[i] == Character.MIN_VALUE) && values[i] > -9999) {
							total += values[i];
							num++;
						}
					}

					// find average
					int avg = num == 0 ? 0 : total / num;

					return year + "," + avg;
				}
		).persist(StorageLevel.MEMORY_ONLY());

		// create key/val pairs used for aggregation, coalescence and sorting
		JavaPairRDD<String, Tuple2<Integer, Integer>> pairRDD = coTemp.mapToPair(s -> {
			String[] tokens = s.split(",");
			return new Tuple2(tokens[0], new Tuple2(Integer.parseInt(tokens[1]), 1));
		});

		// aggregate averages from months to years
		JavaPairRDD<String, Tuple2<Integer, Integer>> tempAverages = pairRDD.reduceByKey(
				(Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>)
						(acc, val) -> new Tuple2(acc._1 + val._1, acc._2 + val._2)
		).coalesce(1, true).sortByKey();

		// map final values for output
		JavaRDD<String> yearlyTemp = tempAverages.map(
				(Function<Tuple2<String, Tuple2<Integer, Integer>>, String>) s -> {
					String year = s._1;
					Integer sum = s._2._1;
					Integer count = s._2._2;
					return year + "," + (sum / count);
				}
		).persist(StorageLevel.MEMORY_ONLY());

		return yearlyTemp;

	}

}

