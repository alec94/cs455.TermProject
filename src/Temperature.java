import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

/**
 * Created by Justin on 4/23/2017.
 */
public class Temperature {

	public static JavaRDD<String> filterTemperature (JavaRDD<Summary> coData, String requestedElement) {

		JavaRDD<String> coTemp = coData.filter(
				(Function<Summary, Boolean>) line ->
					line.getElement().equals(requestedElement)
		).map(
				(Function<Summary, String>) line -> {
					String year = String.valueOf(line.getYear());
					String month = line.getMonth();

					int total = 0, num = 0;
					int[] values = line.getValues();
					char[] qFlags = line.getQFlags();

					for (int i = 0; i < values.length; i++) {
						if ((qFlags[i] == ' ' || qFlags[i] == Character.MIN_VALUE) && values[i] > -9999) {
							total += values[i];
							num++;
						}
					}

					int avg = num == 0 ? 0 : total / num;

					// format: YEAR,MONTH,AVG
					return year + "," + month + "," + avg;
				}
		).persist(StorageLevel.MEMORY_ONLY());

		return coTemp;

	}

}

