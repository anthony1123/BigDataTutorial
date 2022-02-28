package uk.ac.gla.dcs.bigdata.apps;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.functions.flatmapgroups.FilterByPriceAndCountWords;
import uk.ac.gla.dcs.bigdata.functions.map.GameToPlatforms;
import uk.ac.gla.dcs.bigdata.functions.map.SteamStatsFormatter;
import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**
 * Spark Tutorial 3c, illustrates the use of broadcast variable and accumulators.
 * 
 * Task: Group steam games by platform, remove games with an initial price lower than $10 and then prints out those descriptions.
 *       At the same time, calculate the average the number of non-stopword terms within the game descriptions only for games on Mac
 * @author Richard
 *
 */
public class SparkTutorial3c {

	/**
	 * Gets the average price for games grouped my metacritic score using map groups
	 * @return
	 */
	public static void main(String[] args) {
		
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
						.setAppName("SparkTutorial3a");
				
				
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
				
				// Spark needs to understand how to serialise (i.e. package for storage or network transfer) any Java object type we are going to use, since 
				// in a distributed setting our transformations may happen on different machines, or intermediate results may need to be stored between processing
				// stages. We do this by defining an Encoder for the object. In this case, we going to use a new class SteamGameStats, so we need an encoder for it.
				// Encoders.bean() can be used to automatically construct an encoder for an object, so long as the object 1) is not native (e.g. an int or String) and
				// the object is inherently Serializable. If dealing with native Java types then you can use Encoders.<NativeType>(), e.g. Encoders.STRING().
				Encoder<SteamGameStats> steamGameStatsEncoder = Encoders.bean(SteamGameStats.class);
				
				// In Spark, data tranformations are specified by calling transformation functions on Datasets
				// The most basic transformation is 'map', this converts each item in the dataset to a new item (that may be of a different type)
				// The map function takes as input two parameters
				//   - A class that implements MapFunction<InputType,OutputType>
				//   - An encoder for the output type (which we just created in the previous step)
				Dataset<SteamGameStats> steamGames = steamGamesAsRowTable.map(new SteamStatsFormatter(), steamGameStatsEncoder);
				
				//-----------------------------------------
				// Tutorial 3c Additions
				//-----------------------------------------
				
				// Lets assume that we want to figure out the average price of games with different metacritic scores (e.g to see if they
				// are correlated)
				
				// First, we need to group our games based on their platform using a map function
				GameToPlatforms keyFunction = new GameToPlatforms();
			
				// Now we can apply this function to our game list to get a new dataset grouped by this key
				KeyValueGroupedDataset<String, SteamGameStats> gamesByPlatfom = steamGames.groupByKey(keyFunction, Encoders.STRING());
				
				
				// In the next step, we want to filter out any games with an initial price lower than $30, we can do this using a 
				// flatmapgroups function. However, it would also be more efficient to have this function collect our term statistics
				// in the game descriptions at the same time.
				
				// Lets set up for collecting the term statistics. We need to calculate an average, so we need a term count and the number
				// of descriptions, we can use two accumulators for this:
				LongAccumulator wordCountAccumulator = spark.sparkContext().longAccumulator();
				LongAccumulator docCountAccumulator = spark.sparkContext().longAccumulator();
				
				// Note that the task asks for only non-stopword terms to be considered, so we need to get a list of stopwords
				// from somewhere, I have included one in a file, so lets load that
				// (the stopwords list came from https://github.com/terrier-org/terrier-desktop/blob/master/share/stopword-list.txt)
				Set<String> stopwords = new HashSet<String>();
				try {
					BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("data/stopwords.txt")));
					String word = null;
					while ((word = reader.readLine())!=null) stopwords.add(word);
					reader.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				// The next question is how do we send that data to our flatmapgroups function efficiently, lets broadcast it!
				Broadcast<Set<String>> broadcastStopwords = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(stopwords);
				
				// We now have everything we need, lets instantiate our MapGroupsFunction
				FilterByPriceAndCountWords priceFlatMapGroupsFunction = new FilterByPriceAndCountWords(
						wordCountAccumulator, 
						docCountAccumulator, 
						broadcastStopwords, 
						10);
				
				// ... and apply it to our dataset
				Dataset<SteamGameStats> gamesFilteredByPrice = gamesByPlatfom.flatMapGroups(priceFlatMapGroupsFunction, Encoders.bean(SteamGameStats.class));
				

				
				
				//-----------------------------------------
				// Data Collection at the Driver
				//-----------------------------------------
				
				// Collect our data at the driver as a list (Spark ACTION)
				List<SteamGameStats> games = gamesFilteredByPrice.collectAsList();
				
				// Now that an action has been called, we can collect the accumulator data
				long numWordsInMacGameDesc = wordCountAccumulator.value();
				long numGamesAfterFiltering = docCountAccumulator.value();
				
				double averageDescriptionLengthForMacGames = (1.0*numWordsInMacGameDesc)/numGamesAfterFiltering;
				
				spark.close(); // close down the session now we are done with it
				
				// Lets iterate over our data and print it
				for (SteamGameStats game : games) {
					System.out.println(game.getPriceinitial()+" "+game.getDetaileddescrip());
				}
				System.out.println("Average Length of Descriptions: "+averageDescriptionLengthForMacGames);
				
	}
	
}
