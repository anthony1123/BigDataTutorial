package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.functions.map.GameToMetaCriticScore;
import uk.ac.gla.dcs.bigdata.functions.map.PriceMap;
import uk.ac.gla.dcs.bigdata.functions.map.SteamStatsFormatter;
import uk.ac.gla.dcs.bigdata.functions.reducer.PriceSum;
import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

public class SparkTutorial3b {

	/**
	 * Gets the average price for games grouped my metacritic score using a reducer
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
						.setAppName("SparkTutorial3b");
				
				
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
				// Tutorial 3b Additions
				//-----------------------------------------
				
				// Lets assume that we want to figure out the average price of games with different metacritic scores (e.g to see if they
				// are correlated). In this case, we are going to use a reducer, rather than a mapGroups function 
				
				
				
				// First, we need to group our games based on their MetaCritic scores, to do this we are going to need a new MapFunction that extracts a key for
				// for each game (where the key is the metacritic score)
				GameToMetaCriticScore keyFunction = new GameToMetaCriticScore();
				
				// Now we can apply this function to our game list to get a new dataset grouped by this key
				KeyValueGroupedDataset<Integer, SteamGameStats> gamesByMetaCriticScore = steamGames.groupByKey(keyFunction, Encoders.INT());
				
				// To calculate the average prices of games via reducer, we are going to need to define a reducer that performs
				// pair-wise addition of prices, that we will divde by the number of games for each key later to form an average
				PriceSum priceSumReducer = new PriceSum();
				
				// PriceSum is a <double,double> -> double transformation function, so we need to replace our SteamGameStats objects
				// with doubles (prices) before we can start reducing. This can be done with the .mapValues method, which simply
				// applies a normal MapFunction to each value in a KeyValueGroupedDataset
				PriceMap priceMap = new PriceMap();
				
				// Converts KeyValueGroupedDataset<Integer, SteamGameStats> to KeyValueGroupedDataset<Integer, Double>
				KeyValueGroupedDataset<Integer, Double> allScoresAndPrices = gamesByMetaCriticScore.mapValues(priceMap, Encoders.DOUBLE());
				
				// We are going to need the number of games in each bracket to divde by for averaging
				Dataset<Tuple2<Integer,Object>> countsPerKey =  allScoresAndPrices.count();
				
				// Now we can reduce
				Dataset<Tuple2<Integer,Double>> scoresAndPrices = allScoresAndPrices.reduceGroups(priceSumReducer);
				
				//-----------------------------------------
				// Data Collection at the Driver
				//-----------------------------------------
				
				// Collect our data at the driver as a list
				List<Tuple2<Integer,Double>> scoresAndPricesList = scoresAndPrices.collectAsList();
				List<Tuple2<Integer,Object>> countsPerKeyList =  countsPerKey.collectAsList(); // the value of the tuples shows up as a generic object, but they are actually Long numbers
				
				// reformat the counts to make searching them easier
				Map<Integer,Long> countsPerKeyMap = new HashMap<Integer,Long>();
				for (Tuple2<Integer,Object> tuple : countsPerKeyList) countsPerKeyMap.put(tuple._1, (Long)tuple._2);
				
				spark.close(); // close down the session now we are done with it
				
				// Lets iterate over our data and print it
				Iterator<Tuple2<Integer,Double>> tupleIterator = scoresAndPricesList.iterator();
				while (tupleIterator.hasNext()) {
					
					Tuple2<Integer,Double> tuple = tupleIterator.next();
					
					int key = tuple._1;
					long countForKey = countsPerKeyMap.get(key);
					
					System.out.println(tuple._1+" "+tuple._2/countForKey);
					
				}
				
	}
	
}
