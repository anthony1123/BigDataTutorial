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

 *这是第二个Apache Spark教程应用程序（B部分），
 *它包含一个基本的预建Spark应用程序，
 *你可以运行它来确保你的环境设置正确，
 *其中有map、flatmap和reduce function的例子。
 */
public class SparkTutorial2b {

	public SparkTutorial2b() {}

	/**
	 *  在我们的样本中获取 Steam 游戏的平均 metacritic 分数
	 * @param includePC - include PC games
	 * @param includeLinux - include Linux games
	 * @param includeMac - include MacOS games
	 * @return
	 */
	public double getAverageMetaCriticScore(boolean includePC, boolean includeLinux, boolean includeMac) {

		File hadoopDIR = new File("resources/hadoop/");
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath());

		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("SparkTutorial1");

		SparkSession spark = SparkSession
				.builder()
				.config(conf)
				.getOrCreate();

		Dataset<Row> steamGamesAsRowTable = spark
				.read()
				.option("header", "true")
				.csv("data/Steam/games_features.sample.csv");


		
		// Tutorial 2b Note: in this case, 转化为 SteamGameStats 是不高效的, 
		//因为如果我们只想对输入数据集中的单列数字进行平均，
		//那么Spark SQL的内置函数就能非常有效地帮我们完成这个任务。 
		//But we will do it the long way here for illustration.

		Encoder<SteamGameStats> steamGameStatsEncoder = Encoders.bean(SteamGameStats.class);
		
		/*
		 * 在Spark中，数据转换是通过调用数据集的转换函数来指定的。
		 * 最基本的转换是 "map"，它将数据集中的每个项目转换为一个新的项目（可能是不同的类型）。
		 * map函数需要两个参数作为输入 
		 * - 一个实现MapFunction<InputType,OutputType>的类
  		 * - 一个输出类型的编码器（我们在上一步中刚刚创建了这个编码器）
		 */
		
		Dataset<SteamGameStats> steamGames = steamGamesAsRowTable.map(new SteamStatsFormatter(), steamGameStatsEncoder);

		//-----------------------------------------
		// Tutorial 2a Additions
		//-----------------------------------------

		/*
		 * 让我们假设我们只想返回支持MacOS平台的游戏。
		 * 实际上，我们需要对steamGames数据集进行过滤，删除任何不支持MacOS的游戏。
		 * 我们可以使用flatmap函数来完成这个任务，与基本map不同，它可以选择返回0个项目，
		 * 因为它的输出是一个集合的迭代器。
		 */

		// 我已经包含了一个预建的flatmap函数（一个扩展了FlatMapFunction<InputType,OutputType>的类），所以让我们创建一个
		PlatformFilterFlatMap macSupportFilter = new PlatformFilterFlatMap(includePC,includeLinux,includeMac); // pc=false, linux=false, mac=true

		/*
		 * 然后，我们可以通过调用flatmap来获取steamGames数据集并应用我们的过滤器，
		 * 创建一个新的过滤过的数据集。
		 * 和前面的map函数一样，该函数的输出类型是SteamGameStats对象，
		 * 所以我们可以重新使用之前创建的该类型的编码器 
		 */
		Dataset<SteamGameStats> macSteamGames = steamGames.flatMap(macSupportFilter, steamGameStatsEncoder);

		// 现在我们想提取metacritic分数，所以让我们执行一个从SteamGameStats到整数的映射（metacritic分数）。
		Dataset<Integer> metaCriticScores = macSteamGames.map(new SteamGameToMetaCriticScoreMap(), Encoders.INT());
		

		/*
		 * 现在我们有了metacritic的分数，
		 * 我们可以使用一个reducer以并行的方式对它们进行求和 
		 * reduce也是一个动作，所以这将触发到这一点的处理 
		 */
		Integer metaCriticScoreSUM = metaCriticScores.reduce(new IntSumReducer());
		
		// 我们还需要游戏的数量来计算平均数
		long numGames = metaCriticScores.count();
		
		// 现在让我们计算一下平均数
		double averageMetaCriticScore = (1.0*metaCriticScoreSUM)/numGames; // 注意，我在除法前将metaCriticScoreSUM乘以1.0转换为double，否则会发生四舍五入的情况。
		spark.close(); // 关闭会话，现在我们已经完成了它。

		return averageMetaCriticScore;
	}

	/**
	 * 这是主方法，它将建立一个新的本地Spark会话
	 * 并运行示例应用程序以检查环境是否设置正确。
	 * @param args
	 */
	public static void main(String[] args) {

		SparkTutorial2b example2b = new SparkTutorial2b();

		double averageMetaCriticScore = example2b.getAverageMetaCriticScore(false,false,true); // pc=false, linux=false, mac=true

		System.out.println("Average MetaCritic Score: "+averageMetaCriticScore);




	}

}
