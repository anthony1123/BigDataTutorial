package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import uk.ac.gla.dcs.bigdata.functions.flatmap.PlatformFilterFlatMap;
import uk.ac.gla.dcs.bigdata.functions.map.SteamGameToMetaCriticScoreMap;
import uk.ac.gla.dcs.bigdata.functions.map.SteamStatsFormatter;
import uk.ac.gla.dcs.bigdata.functions.reducer.IntSumReducer;
import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**
 * This is the second Apache Spark tutorial application (part B), it contains a basic pre-built Spark
 * app that you can run to make sure that your environment is set up correctly, with examples
 * of both a map, flatmap and reduce function.
 * @author Richard
 *
 */
public class SparkTutorial2b {

	public SparkTutorial2b() {}

	/**
	 * Get the average metacritic score of steam games in our sample
	 * @param includePC - include PC games
	 * @param includeLinux - include Linux games
	 * @param includeMac - include MacOS games
	 * @return
	 */
	public double getAverageMetaCriticScore(boolean includePC, boolean includeLinux, boolean includeMac) {

		// Spark relies on some legacy components of hadoop to manage data input/output, this means we need to
		// give Spark a copy of some hadoop executables, in this case we have included a copy of these in the
		// resources/hadoop/ directory.
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it

		// SparkConf is the configuration for the spark session we are going to create
		// setMaster specifies how we want the application to be run, in this case in local mode with 2 cores
		// setAppName specifies the name of the spark session we will create, this is in effect a unique identifier for our session
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("SparkTutorial1");


		// To run a Spark job we need to create a SparkSession, in local mode, this creates a temporary spark cluster
		// on your local machine to run the job on.
		SparkSession spark = SparkSession
				.builder()
				.config(conf)
				.getOrCreate();

		// --------------------------------------------------------------------------------------
		// Spark Application Topology Starts Here
		// --------------------------------------------------------------------------------------

		//-----------------------------------------
		// Data Input
		//-----------------------------------------
		// Lets read in some statistics of Steam games from a file
		Dataset<Row> steamGamesAsRowTable = spark
				.read()
				.option("header", "true")
				.csv("data/Steam/games_features.sample.csv");

		
		
		// Row objects are a general representation of data items in Spark, a Dataset<Row> represents a set of Rows, i.e. its like a Table
		// Dataset<Row> support lots of out-of-the-box transformations using Spark SQL, but as we are more interested in what happens 'under-the-hood'
		// in Spark, we will be converting our Rows to more specialist data types
		
		// Tutorial 2b Note: in this case, doing the conversion to SteamGameStats is inefficient, since if we are only looking to average numbers in a
		// single column of the input dataset then Spark SQL's in-built functions would be able to do this very efficiently for us. But we will do it the
		// long way here for illustration.
		
		//-----------------------------------------
		// Data Transformations
		//-----------------------------------------

		// As a simple test, lets convert each Row object to a SteamGameStats object, such that we have easier access to get/set methods for each

		// Spark needs to understand how to serialize (i.e. package for storage or network transfer) any Java object type we are going to use, since 
		// in a distributed setting our transformations may happen on different machines, or intermediate results may need to be stored between processing
		// stages. We do this by defining an Encoder for the object. In this case, we going to use a new class SteamGameStats, so we need an encoder for it.
		// Encoders.bean() can be used to automatically construct an encoder for an object, so long as the object 1) is not native (e.g. an int or String) and
		// the object is inherently Serializable. If dealing with native Java types then you can use Encoders.<NativeType>(), e.g. Encoders.STRING().
		Encoder<SteamGameStats> steamGameStatsEncoder = Encoders.bean(SteamGameStats.class);

		// In Spark, data tranformations are specifed by calling transformation functions on Datasets
		// The most basic transformation is 'map', this converts each item in the dataset to a new item (that may be of a different type)
		// The map function takes as input two parameters
		//   - A class that implements MapFunction<InputType,OutputType>
		//   - An encoder for the output type (which we just created in the previous step)
		Dataset<SteamGameStats> steamGames = steamGamesAsRowTable.map(new SteamStatsFormatter(), steamGameStatsEncoder);

		//-----------------------------------------
		// Tutorial 2a Additions
		//-----------------------------------------

		// Lets assume that we only want to return games that support MacOS as a platform. In effect we need to filter the steamGames dataset to remove
		// any games that do not support MacOS. We can do this using a flatmap function, which unlike a basic map, can opt to return 0 items since its
		// output is an iterator over a collection.

		// I have included a pre-built flatmap function (a class that extends FlatMapFunction<InputType,OutputType>), so lets create one
		PlatformFilterFlatMap macSupportFilter = new PlatformFilterFlatMap(includePC,includeLinux,includeMac); // pc=false, linux=false, mac=true

		// We can then take the steamGames dataset and apply our filter by calling flatmap, creating a new filtered dataset
		// As with our map function earlier, the output type of the function are SteamGameStats objects, so we can re-use the encoder for that type created earlier 
		Dataset<SteamGameStats> macSteamGames = steamGames.flatMap(macSupportFilter, steamGameStatsEncoder);

		// Now we want to extract the metacritic scores, so lets perform a map from SteamGameStats to an integer (the metacritic score)
		Dataset<Integer> metaCriticScores = macSteamGames.map(new SteamGameToMetaCriticScoreMap(), Encoders.INT());
		
		// Now that we have the metacritic scores, we can use a reducer to sum them in a parallel fashion
		// reduce is also an action, so this will trigger processing up to this point 
		Integer metaCriticScoreSUM = metaCriticScores.reduce(new IntSumReducer());
		
		// We also need the number of games to calculate the average...
		long numGames = metaCriticScores.count();
		
		// Now lets calculate the average
		double averageMetaCriticScore = (1.0*metaCriticScoreSUM)/numGames; // note that I multiply metaCriticScoreSUM by 1.0 to convert it to a double before division, otherwise rounding will happen
		
		spark.close(); // close down the session now we are done with it

		return averageMetaCriticScore;
	}

	/**
	 * This is the main method that will set up a new local Spark session
	 * and run the example app to check that the environment is set up
	 * correctly.
	 * @param args
	 */
	public static void main(String[] args) {

		SparkTutorial2b example2b = new SparkTutorial2b();

		double averageMetaCriticScore = example2b.getAverageMetaCriticScore(false,false,true); // pc=false, linux=false, mac=true

		System.out.println("Average MetaCritic Score: "+averageMetaCriticScore);




	}

}
