import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

/**
 * Created by Justin on 4/23/2017.
 */
public class Temperature {

	public static JavaRDD<String> filterTemperature(JavaRDD<Summary> coData) {

		JavaRDD<String> coTemp = coData.filter(
				(Function<Summary, Boolean>) line ->
					line.getElement().equals("TMIN") || line.getElement().equals("TMAX")
		).map(
				(Function<Summary, String>) line -> {
					String year = String.valueOf(line.getYear());
					String month = line.getMonth();
					String element = line.getElement();
					int[] values = line.getValues();
					char[] qFlags = line.getQFlags();

					int totalMin = 0, numMin = 0;
					int totalMax = 0, numMax = 0;
					for (int i = 0; i < values.length; i++) {
						if ((qFlags[i] == ' ' || qFlags[i] == Character.MIN_VALUE) && values[i] > -9999) {
							if (element.equals("TMIN")) {
								totalMin += values[i];
								numMin++;
							} else {
								totalMax += values[i];
								numMax++;
							}
						}
					}

					int avgMin = totalMin / numMin;
					int avgMax = totalMax / numMax;

					// format: YEAR,MONTH,AVGMIN,AVGMAX
					return year + "," + "," + month + "," + avgMin + "," + avgMax;
				}
		).persist(StorageLevel.MEMORY_ONLY());

		return coTemp;

	}

}

