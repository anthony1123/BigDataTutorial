package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import uk.ac.gla.dcs.bigdata.functions.flatmap.RequirementsFilter;
import uk.ac.gla.dcs.bigdata.functions.map.SteamGamesToListMap;
import uk.ac.gla.dcs.bigdata.functions.map.SteamStatsFormatter;
import uk.ac.gla.dcs.bigdata.functions.reducer.SameTitleReducer;
import uk.ac.gla.dcs.bigdata.structures.SteamGameList;
import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

public class Task1SolutionFlatMap {

	
	public static void main(String[] args) {
		
		//-----------------------------------------
		// Standard Spark Setup
		//-----------------------------------------
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("SparkTutorial2a");
		
		
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
		
		
		//-----------------------------------------
		// Data Input
		//-----------------------------------------
		// Lets read in some statistics of Steam games from a file
		Dataset<Row> steamGamesAsRowTable = spark
				.read()
				.option("header", "true")
				.csv("data/Steam/games_features.sample.csv");

		
		//-----------------------------------------
		// Data Transformations
		//-----------------------------------------
		
		// Encoder for SteamGameStats
		Encoder<SteamGameStats> steamGameStatsEncoder = Encoders.bean(SteamGameStats.class);
		
		// Map each Row to SteamGameStats
		Dataset<SteamGameStats> steamGames = steamGamesAsRowTable.map(new SteamStatsFormatter(), steamGameStatsEncoder);
		
		// Filter steamGames by minimum requirements using a flatmap
		Dataset<SteamGameStats> xpGames = steamGames.flatMap(new RequirementsFilter(), steamGameStatsEncoder);
		
		// We are going to use a reducer to remove games with identical titles. To do this, we need a data structure
		// that can hold multiple steam games, as reducers do pair-wise comparisons. So we can create a basic Java object
		// containing a list of steam games for this (SteamGameList), and convert each game to a game list.
		Encoder<SteamGameList> steamGameStatsListEncoder = Encoders.bean(SteamGameList.class);
		Dataset<SteamGameList> xpGamesAsLists =  xpGames.map(new SteamGamesToListMap(), steamGameStatsListEncoder);
		
		
		//-----------------------------------------
		// Spark Action (reduce)
		//-----------------------------------------
		
		// Now lets reduce down to a single steam game list, by removing games with identical titles
		SteamGameList uniqueXPGamesAsLists = xpGamesAsLists.reduce(new SameTitleReducer());
		
		// Print out the count
		System.out.println(uniqueXPGamesAsLists.getGameList().size());
		
		// Sort by recommendation count because I want to
		Collections.sort(uniqueXPGamesAsLists.getGameList());
		Collections.reverse(uniqueXPGamesAsLists.getGameList());
		
		// Print the titles
		for (SteamGameStats game : uniqueXPGamesAsLists.getGameList()) {
			System.out.println(game.getTitle());
		}
		
		
	}
	
	
}
