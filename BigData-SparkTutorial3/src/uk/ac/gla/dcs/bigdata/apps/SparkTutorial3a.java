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
		
				 
				File hadoopDIR = new File("resources/hadoop/");
				System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath());
				
				// SparkConf是我们要创建的火花会话的配置。
				// setMaster指定了我们希望应用程序的运行方式，在本例中是本地模式下的2个核心。
				// setAppName指定我们将创建的spark会话的名称，这实际上是我们会话的唯一标识符。
				SparkConf conf = new SparkConf()
						.setMaster("local[2]")
						.setAppName("SparkTutorial3a");

				SparkSession spark = SparkSession
						  .builder()
						  .config(conf)
						  .getOrCreate();
						
				// --------------------------------------------------------------------------------------
				// Spark Application Topology Starts Here
				// --------------------------------------------------------------------------------------

				Dataset<Row> steamGamesAsRowTable = spark
						.read()
						.option("header", "true")
						.csv("data/Steam/games_features.sample.csv");

				
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
				
				/*
				 * 在Spark中，数据转换是通过调用数据集的转换函数来指定的。
				 * 最基本的转换是 "map"，它将数据集中的每个项目转换为一个新的项目（可能是不同的类型）。
				 * map函数接受两个参数作为输入。
				 * 	- 一个实现MapFunction<InputType,OutputType>的类。
				 * 	- 一个输出类型的编码器（我们在上一步中刚刚创建了这个编码器）
				 */
				Dataset<SteamGameStats> steamGames = steamGamesAsRowTable.map(new SteamStatsFormatter(), steamGameStatsEncoder);
				
				//-----------------------------------------
				// Tutorial 3a Additions
				//-----------------------------------------
				
				/*
				 * 假设我们想算出具有不同metacritic分数的游戏的平均价格（例如，看它们是否有关联）。
				 * 首先，我们需要根据游戏的MetaCritic分数对其进行分组，
				 * 为此我们需要一个新的MapFunction，为每个游戏提取一个键（其中键是metacritic分数）
				 */
				GameToMetaCriticScore keyFunction = new GameToMetaCriticScore();
				
				// 现在，我们可以将这个函数应用于我们的游戏列表，以获得一个按该键分组的新数据集
				KeyValueGroupedDataset<Integer, SteamGameStats> gamesByMetaCriticScore = steamGames.groupByKey(keyFunction, Encoders.INT());
				
				// 我们想计算每个metacritic分数段的平均价格，为此我们需要实现一个新的MapGroupsFunction，它将汇总每个键（metacritic分数）的所有游戏。
				AVGPrice priceAggregator = new AVGPrice();				
				/*
				 * 重要的是，为了使上述聚合的输出有用，我们希望同时返回metacritic分数和平均价格。
				 * 我们可以定义我们自己的java类来存储这些信息，但是我们可以使用Spark内置的Tuple规范来做到这一点，即不返回Dataset<MyType>，
				 * 而是返回Dataset<Tuple2<Integer,Double>>，这就是AVGPrice的作用。值得注意的是，我们将需要一个Tuple2的编码器，
				 * 幸运的是Encoders类为我们提供了一个建立TupleX编码器的方法。在这种情况下，.tuple方法根据元组中包含的类型的编码器来建立一个元组编码器。
				 */
				Encoder<Tuple2<Integer,Double>> scorePriceEncoder = Encoders.tuple(Encoders.INT(), Encoders.DOUBLE());
				// 现在我们可以应用它来得到一组每个价格段的游戏的平均价格
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
