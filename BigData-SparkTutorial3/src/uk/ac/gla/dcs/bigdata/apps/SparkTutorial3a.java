package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.functions.map.GameToMetaCriticScore;
import uk.ac.gla.dcs.bigdata.functions.map.SteamStatsFormatter;
import uk.ac.gla.dcs.bigdata.functions.mapgroups.AVGPrice;
import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

public class SparkTutorial3a {

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
				// Tutorial 3a Additions
				//-----------------------------------------
				
				// Lets assume that we want to figure out the average price of games with different metacritic scores (e.g to see if they
				// are correlated)
				
				// First, we need to group our games based on their MetaCritic scores, to do this we are going to need a new MapFunction that extracts a key for
				// for each game (where the key is the metacritic score)
				GameToMetaCriticScore keyFunction = new GameToMetaCriticScore();
				
				// Now we can apply this function to our game list to get a new dataset grouped by this key
				KeyValueGroupedDataset<Integer, SteamGameStats> gamesByMetaCriticScore = steamGames.groupByKey(keyFunction, Encoders.INT());
				
				// We want to calculate the average price for each metacritic score bracket, to do that we are going to need to
				// implement a new MapGroupsFunction that will aggregate all games for each key (metacritic score)
				AVGPrice priceAggregator = new AVGPrice();
				
				// Importantly, to make the output of the above aggregation useful, we want to return both the metacritic score AND the average
				// price. We could define our own java class for storing this information, but we can use Spark's built-in Tuple specification
				// to do this, i.e. instead of returning a Dataset<MyType>, we instead return a Dataset<Tuple2<Integer,Double>>, which is what
				// AVGPrice does.
				
				// Notably, we will need an encoder for Tuple2, luckily the Encoders class provides us a method to build TupleX encoders:
				// In this case the .tuple method builds a Tuple encoder based on encoders for the types contained within the tuple.
				Encoder<Tuple2<Integer,Double>> scorePriceEncoder = Encoders.tuple(Encoders.INT(), Encoders.DOUBLE());
				

				// Now we can apply it to get a set of the average price of games in each price bracket
				Dataset<Tuple2<Integer,Double>> scoresAndPrices = gamesByMetaCriticScore.mapGroups(priceAggregator, scorePriceEncoder);

				//-----------------------------------------
				// Data Collection at the Driver
				//-----------------------------------------
				
				// Collect our data at the driver as a list
				List<Tuple2<Integer,Double>> scoresAndPricesList = scoresAndPrices.collectAsList();
				
				spark.close(); // close down the session now we are done with it
				
				// Lets iterate over our data and print it
				Iterator<Tuple2<Integer,Double>> tupleIterator = scoresAndPricesList.iterator();
				while (tupleIterator.hasNext()) {
					
					Tuple2<Integer,Double> tuple = tupleIterator.next();
					
					System.out.println(tuple._1+" "+tuple._2);
					
				}
				
	}
	
	
}
