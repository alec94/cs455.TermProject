import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Alec on 4/21/2017.
 * main for term project
 */
public class Main {

	private static String usage = "usage: <climate data path> <station data path> <output path> <element> <scope>" +
			"\telements: snow, tavg\n\tscope: co, fr, rm";

	private static String dataPath;
	private static String stationsPath;

	// front range rectangle
	private static final double bellvueLatitude = 40.671869, bellvueLongitude = -105.445208,
			fountainLatitude = 38.644019, fountainLongitude = -104.729490;

	// rocky mountain rectangle, surrounds most ski areas
	private static final double clarkLatitude = 40.705538, clarkLongitude = -106.919242,
			tarryallLatitude = 39.119314, tarryallLongitude = -105.473954;

	// great plains rectangle, pretty much all eastern-colorado agriculture
	private static final double norfolkLatitude = 40.991869, norfolkLongitude = -105.232388,
			threePointsLatitude = 36.994665, threePointsLongitude = -102.047930;

	// fort collins rectangle
	private static final double laPorteLatitude = 40.621286, laPorteLongitude = -105.155840,
			windsorLatitude = 40.471282, windsorLongitude = -104.974364;

	// initializes workable Summary objects from raw files
	private static JavaRDD<Summary> initData(JavaSparkContext sparkContext, String scope){

		JavaRDD<String> rawData = sparkContext.textFile(dataPath);
		JavaRDD<String> stations = sparkContext.textFile(stationsPath);
		Object[] coStations;

		if(scope.equals("co")) {

			// get ids of stations in colorado
			coStations = stations.filter(
					(Function<String, Boolean>) line -> line.substring(38, 40).trim().equals("CO")
			).map(
					(Function<String, String>) line -> line.substring(0, 11)
			).collect().toArray();

		} else { // scope fr, rm, gp, fc
			coStations = stations.filter(
					(Function<String, Boolean>) line -> {
						double latitude = Double.parseDouble(line.substring(12, 20).trim());
						double longitude = Double.parseDouble(line.substring(21, 30).trim());
						boolean isInScope = false;

						double scopeNWLatitude = 0, scopeNWLongitude = 0, scopeSELatitude = 0, scopeSELongitude = 0;
						switch(scope) {
							case "fr": // scopes to front range
								scopeNWLatitude = bellvueLatitude; scopeNWLongitude = bellvueLongitude;
								scopeSELatitude = fountainLatitude; scopeSELongitude = fountainLongitude;
								break;
							case "rm": // scopes to rocky mountains
								scopeNWLatitude = clarkLatitude; scopeNWLongitude = clarkLongitude;
								scopeSELatitude = tarryallLatitude; scopeSELongitude = tarryallLongitude;
								break;
							case "gp": // scopes to great plains
								scopeNWLatitude = norfolkLatitude; scopeNWLongitude = norfolkLongitude;
								scopeSELatitude = threePointsLatitude; scopeSELongitude = threePointsLongitude;
								break;
							case "fc": // scopes to fort collins
								scopeNWLatitude = laPorteLatitude; scopeNWLongitude = laPorteLongitude;
								scopeSELatitude = windsorLatitude; scopeSELongitude = windsorLongitude;
								break;
							default: // main already error checks input
								break;
						}
						// checks station's coordinates with chosen scope
						if(latitude < scopeNWLatitude && latitude > scopeSELatitude)
							if(longitude < scopeSELongitude && longitude > scopeNWLongitude)
								isInScope = true;

						return isInScope;
					}
			).map(
					(Function<String, String>) line -> line.substring(0, 11)
			).collect().toArray();
		}

		// summarize data into summary objects
		JavaRDD<Summary> coData = rawData.filter(
				(Function<String, Boolean>) line -> Arrays.asList(coStations).contains(line.substring(0,11)) && !line.substring(11,15).trim().equals("-9999")
		).map((Function<String, Summary>) line -> {
			Summary summary = new Summary();
			summary.setID(line.substring(0, 11));
			summary.setYear(Integer.parseInt(line.substring(11, 15).trim()));
			summary.setMonth(line.substring(15, 17));
			summary.setElement(line.substring(17, 21));
			int startV = 21, startM = 26, startQ = 27, startS = 28;

			for (int i = 1; i <= 31; i++){
				summary.setValue(i, Integer.parseInt(line.substring(startV,startV + 5).trim()));
				summary.setMFlag(i, line.charAt(startM));
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

	private static void filterScope(JavaSparkContext sc, String outPath, String element, String scope) {

		// error handling
		ArrayList<String> elements = new ArrayList<>(Arrays.asList("snow", "tavg"));
		ArrayList<String> scopes = new ArrayList<>(Arrays.asList("co", "fr", "rm", "gp", "fc"));
		if(!(elements.contains(element) && scopes.contains(scope))) {
			// bad input, error and return
			System.out.println("ERROR: Bad element or scope input\n" + usage);
			System.exit(-1);
		}

		// filter stuff
		JavaRDD<Summary> rawScopedData = initData(sc, scope);
		JavaRDD<String> scopedData;
		if(element.equals("snow")) {
			scopedData = Snowfall.filterSnowfall(rawScopedData);
		} else { // element.equals("tavg")
			scopedData = Temperature.filterTemperature(rawScopedData, "TAVG");
		}

		// finish up
		System.out.println("lines: " + scopedData.count());
		scopedData.coalesce(1, true).saveAsTextFile(outPath + "/" + element + "/" + scope);
		scopedData.unpersist();
		rawScopedData.unpersist();

	}

	public static void main(String[] args) {

		// ensure all arguments are being used
		if (args.length != 5) {
			System.out.println("ERROR: not enough arguments\n" + usage);
			System.exit(-1);
		}

		// fill argument variables
		dataPath = args[0].trim();
		stationsPath = args[1].trim();
		String outPath = args[2].trim();
		String element = args[3].toLowerCase().trim();
		String scope = args[4].toLowerCase().trim();

		// config and context setup
		SparkConf conf = new SparkConf().setAppName("cs455 Term Project");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// runs filter
		filterScope(sc, outPath, element, scope);

		// finish
		sc.stop();

	}
}
