

public class Snowfall {

	public JavaRDD<String> filterSnowfall(JavaRDD<Summary> coData) {
		// get total monthly snowfall
		JavaRDD<String> coSnowfall = coData.filter(
				(Function<Summary, Boolean>) line -> line.getElement().equals("SNOW")
		).map(
				(Function<Summary, String>) line -> {
					String id = line.getID();
					String date = line.getYear() + "/" + line.getMonth();
					int snowfall = 0;
					int[] values = line.getValues();
					char[] qFlags = line.getQFlags();
					for(int i = 0; i < values.length; i++) {
						// ensure values are valid
						if((qFlags[i] == ' ' || qFlags[i] == null) && values[i] > 0) {
							snowfall += values[i];
						}
					}

					return id + " " + date + ": " + snowfall;
				}
		);

		return coSnowfall;
	}

}