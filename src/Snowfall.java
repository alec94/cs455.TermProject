import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

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
					for(int i = 0; i < values.length; i++) {
						// ensure values are valid
						if((qFlags[i] == ' ' || qFlags[i] == Character.MIN_VALUE) && values[i] > 0) {
							snowfall += values[i];
						}
					}

					return year + "," + month + "," + snowfall;
				}
		).persist(StorageLevel.MEMORY_ONLY());

		coSnowfall.coalesce(1,true).saveAsTextFile("hdfs://denver:30321/455TP/snow-out/");

		coSnowfall.unpersist();

		return coSnowfall;
	}

}