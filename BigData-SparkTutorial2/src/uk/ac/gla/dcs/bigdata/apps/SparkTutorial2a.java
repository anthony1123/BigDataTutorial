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

import uk.ac.gla.dcs.bigdata.functions.flatmap.PlatformFilterFlatMap;
import uk.ac.gla.dcs.bigdata.functions.map.SteamStatsFormatter;
import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**
 * @author Richard
 * 这是第二个 Apache Spark 教程应用程序（B 部分），
 * 它包含一个基本的预构建(pre-built) Spark 应用程序，您可以运行该应用程序以确保您的环境设置正确，
 * 并提供了示例both a map and flatmap function。
 */
public class SparkTutorial2a {

	public SparkTutorial2a() {}
	
	/**
	 * 按推荐数量对 Steam 游戏进行排名，包括按支持的平台过滤
	 * @param includePC - include PC games
	 * @param includeLinux - include Linux games
	 * @param includeMac - include MacOS games
	 * @return
	 */
	public List<SteamGameStats> getRankSteamGames(boolean includePC, boolean includeLinux, boolean includeMac) {
		

				File hadoopDIR = new File("resources/hadoop/");
				System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath());

				SparkConf conf = new SparkConf()
						.setMaster("local[2]")
						.setAppName("SparkTutorial2a");
				
				SparkSession spark = SparkSession
						  .builder()
						  .config(conf)
						  .getOrCreate();

				Dataset<Row> steamGamesAsRowTable = spark
						.read()
						.option("header", "true")
						.csv("data/Steam/games_features.sample.csv");

				Encoder<SteamGameStats> steamGameStatsEncoder = Encoders.bean(SteamGameStats.class);
	
				Dataset<SteamGameStats> steamGames = steamGamesAsRowTable.map(new SteamStatsFormatter(), steamGameStatsEncoder);
				
				//-----------------------------------------
				// Tutorial 2a Additions
				//-----------------------------------------

				/*
				 * 假设我们只想返回支持 Macos 作为平台的游戏。实际上，我们需要过滤 steamGames 数据集以删除任何不支持 Macos 的游戏。
				 * 我们可以使用 flatmap 函数来做到这一点，与基本map function不同，它可以选择返回 0 个项目，因为它的输出是集合上的迭代器(iterator)。
				 * 首先，让我们看看我们总共有多少游戏，可以使用 dataset.count() 方法(这是一个spark动作)
				 * 所以所有创建被统计对象（本例中为Dataset<SteamGameStats> steamGames）所需的处理都会在调用统计前完成。
				 */
				long numGames = steamGames.count();
				
				// lets print that
				System.out.println("NumberOfGames: "+numGames);
		
				
				// I have included a pre-built flatmap function (a class that extends FlatMapFunction<InputType,OutputType>), so lets create one
				PlatformFilterFlatMap macSupportFilter = new PlatformFilterFlatMap(includePC,includeLinux,includeMac); // pc=false, linux=false, mac=true
				
				// We can then take the steamGames dataset and apply our filter by calling flatmap, creating a new filtered dataset
				// As with our map function earlier, the output type of the function are SteamGameStats objects, so we can re-use the encoder for that type created earlier 
				Dataset<SteamGameStats> macSteamGames = steamGames.flatMap(macSupportFilter, steamGameStatsEncoder);
				
				// Now lets check the new game count
				long numMacGames = macSteamGames.count();
				
				// ... and print it
				System.out.println("NumberOfMacGames: "+numMacGames);
				
				//-----------------------------------------
				// Data Collection at the Driver
				//-----------------------------------------
				
				// So far we have been defining variables that hold Java objects of type Dataset<Something>. However, it is important to realize that
				// these objects are not like normal Java objects lists or arrays that we can iterate over and manipulate locally. You can consider these
				// objects as references or pointers to our desired data, since that data might be spread out over lots of machines that did the processing
				// work. If we want to work normally with that data we need to 'collect' it, i.e. ask Spark to get all the parts where-ever they are and 
				// convert it to a normal java collection type like a List. This is what the collectAsList() method does:
				List<SteamGameStats> steamGamesList = macSteamGames.collectAsList();
				
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
		
		SparkTutorial2a example1 = new SparkTutorial2a();
		
		List<SteamGameStats> steamGamesList = example1.getRankSteamGames(false,false,true); // pc=false, linux=false, mac=true
		
		// Print the titles of the top 10 games in our sample
		for (int gameIndex = 0; gameIndex<10; gameIndex++) { // loop over the first 10 games now that they are sorted
			SteamGameStats game = steamGamesList.get(gameIndex);
			String gameTitle = game.getTitle(); // get the title
			
			// lets print out game support to check
			StringBuilder builder = new StringBuilder(); // a string builder is a more efficient structure if building strings from multiple components
			builder.append(gameIndex+1);
			builder.append(": ");
			builder.append(gameTitle);
			builder.append(" [supportsPC=");
			builder.append(game.isPlatformwindows());
			builder.append(", supportsLinux=");
			builder.append(game.isPlatformlinux());
			builder.append(", supportsMacOS=");
			builder.append(game.isPlatformmac());
			builder.append("]");
			
			System.out.println(builder.toString()); // print to standard output
		}
		
		
		
		
	}
	
}
