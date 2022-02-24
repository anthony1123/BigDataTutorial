package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import uk.ac.gla.dcs.bigdata.functions.map.SteamStatsFormatter;
import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**
 * This is the first Apache Spark tutorial application, it contains a basic pre-built Spark
 * app that you can run to make sure that your environment is set up correctly.
 * @author Richard
 *
 */
public class SparkTutorial1 {

	public SparkTutorial1() {}
	
	public List<SteamGameStats> getRankSteamGames() {
		
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
				// Data Collection at the Driver
				//-----------------------------------------
				
				// So far we have been defining variables that hold Java objects of type Dataset<Something>. However, it is important to realize that
				// these objects are not like normal Java objects lists or arrays that we can iterate over and manipulate locally. You can consider these
				// objects as references or pointers to our desired data, since that data might be spread out over lots of machines that did the processing
				// work. If we want to work normally with that data we need to 'collect' it, i.e. ask Spark to get all the parts where-ever they are and 
				// convert it to a normal java collection type like a List. This is what the collectAsList() method does:
				List<SteamGameStats> steamGamesList = steamGames.collectAsList();
				
				// steamGamesList is now a real java list object that we can analyse, e.g. find the 10 most popular games
				Collections.sort(steamGamesList); // default ordering of SteamGameStats is by recommendation count, so sort by this
				Collections.reverse(steamGamesList); // Collections.sort() sorts in ascending order, reverse to get descending order of recommendation count
		
				
				spark.close(); // close down the session now we are done with it
				
				return steamGamesList;
	}
	
	/**
	 * This is the main method that will set up a new local Spark session
	 * and run the example app to check that the environment is set up
	 * correctly.
	 * @param args
	 */
	public static void main(String[] args) {
		
		SparkTutorial1 example1 = new SparkTutorial1();
		
		List<SteamGameStats> steamGamesList = example1.getRankSteamGames();
		
		// Print the titles of the top 10 games in our sample
		for (int gameIndex = 0; gameIndex<10; gameIndex++) { // loop over the first 10 games now that they are sorted
			SteamGameStats game = steamGamesList.get(gameIndex);
			String gameTitle = game.getTitle(); // get the title
			System.out.println((gameIndex+1)+": "+gameTitle); // print to standard output
		}
		
		
		
		
	}
	
}
