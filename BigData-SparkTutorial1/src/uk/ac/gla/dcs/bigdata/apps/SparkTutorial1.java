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
 *Hadoop comes with a range of well tools to enable batch processing of large amounts of data，Hadoop 提供了一系列良好的工具来支持批量处理大量数据
 */
public class SparkTutorial1 {
	
	//------------------------- Pre-step ---------------------------

	public SparkTutorial1() {}//empty constructor
	
	public List<SteamGameStats> getRankSteamGames() {
		//it has a single method called rank steam games which is going to do all of the work
		//获取数据data set
		//将input data转换为java list，然后返回list
		//using spark来加速处理进程
		
		//由于Spark依赖hadoop的一些遗留组件来管理数据输入/输出，
		//这意味着我们需要给 Spark 一些 hadoop 可执行文件的副本，
		//我们在 resources/hadoop/ 目录中包含了这些副本。
		//导入一些一些 hadoop可执行文件的副本
				File hadoopDIR = new File("resources/hadoop/"); // 获得hadoop目录的绝对路径并表示为Java file类
				System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // 设置JVM system property，使Spark能找到jvm 
				
				//为启动spark集群cluster做配置
				SparkConf conf = new SparkConf()//SparkConf是我们将要创建的 spark 会话的配置
						.setMaster("local[2]") //setMaster 指定cluster的类型，这里是cluster本地模式下具有 2 个核心
						.setAppName("SparkTutorial1");//setAppName 指定我们将创建的 spark session会话的名称，是会话的唯一标识符
				
				
				/*This is now setting up the spark cluster or connecting to the spark cluster if it was remote.
				 * 这是现在设置spark集群或连接到远程spark集群。
				 * 一旦我们有了我们的配置。然后我们需要创建这个叫做 SparkSession 的东西，这就是它的真正含义。
				 * 要运行 Spark 作业，我们需要在本地模式下创建一个 SparkSession，这会在您的本地计算机上创建一个临时 Spark 集群来运行该作业。
				 */
				//使用 sparksession.builder()创建一个新的spark会话，然后传入配置文件，
				SparkSession spark = SparkSession
						  .builder()
						  .config(conf)
						  .getOrCreate();//检查是否已经存在具有此名称的特定Spark会话，没有则创建
				

				

				// --------------------------------------------------------------------------------------
				// Spark Application Topology Starts Here, Spark 应用拓扑从这里开始
				// --------------------------------------------------------------------------------------
	
				
				
				//------------------------- 数据导入 ---------------------------	
				Dataset<Row> steamGamesAsRowTable = spark
						.read()
						.option("header", "true")
						.csv("data/Steam/games_features.sample.csv");
				// 行是Spark 中的一种一般表示，因此您可以有效地表示大多数类型信息
				// a Dataset<Row> 表示一组行a list of Rows，即它像一个表Table
				// spark 提供了许多有用的预构建方法，并使用 spark SQL 使用 row 类型的数据集(Dataset<Row>)进行out-of-the-box transformations
				
				

				
				//数据导入Rows，转换为自定义对象序列化，再转换数据类型
				/*
				 * 它需要有一个你知道的编码器告诉它如何看到真实的实现这些对象。 
				 * Spark 需要的第二件事是它需要有一个函数。这实际上是要进行转换，这样你就可以完成这项工作，
				 * 所以在这种情况下，我们将使用 map 函数，
				 * 因此，如果您从关于 map 的讨论中回想一下，生成一个 map 函数是它接收的一对一函数，一个对象然后将其映射到另一个对象
				 */
				
				
				
				
				//------------------------- 数据转换 ---------------------------	
				
				
				// 将 Row 对象转换为 SteamGameStats 对象, 方便使用 get/set methods
				// Spark 需要了解怎么 serialize（序列化）(即用于存储或网络传输的包) Java 对象, 因为在分布式设置中，我们的转换可能发生在不同的机器上，或者可能需要存储中间结果处理阶段。
				// 基本上什么是序列化是将内存中存在的Java对象的表示形式，然后将其转换为可以存储在磁盘上store on disk或通过网络传输的形式.
				
				// 我们通过为对象定义一个编码器来做到这一点。 在这种情况下，我们将使用一个新类 SteamGameStats，因此我们需要一个编码器。 
				// Encoders.bean() 可用于自动构造对象的编码器， 
				// 只要对象不是原生对象（例如 int 或 String），对象本质上是可序列化的。
				// 如果处理本机 Java 类型，那么您可以使用编码器。 Encoders.<NativeType>(), e.g. Encoders.STRING().
				
				
				Encoder<SteamGameStats> steamGameStatsEncoder = Encoders.bean(SteamGameStats.class);
				
				// 在 Spark 中，数据转换是通过调用数据集上的转换函数来指定的。最基本的转换是 "map"，它将数据集中的每个项目转换为一个新的项目（可能是不同的类型）。
				// map函数的输入是两个参数。 
				//	 一个实现MapFunction<InputType,OutputType>的类。 
				//	 一个输出类型的编码器（我们刚刚在上一步创建了这个编码器。）
				// 将每一行转换为一个 Java 对象
				//将Dataset<Row>转换为Dataset<SteamGameStats> 
				Dataset<SteamGameStats> steamGames = steamGamesAsRowTable.map(new SteamStatsFormatter(), steamGameStatsEncoder);
				
				//that is how we execute a custom map function inside Spark
				//-----------------------------------------------------------	
				
				
				
				
				//-----------------------------------------
				// 驱动程序的数据收集
				//-----------------------------------------
				

				
				/*
				 * 到目前为止，我们已经定义了保存 Dataset 类型的 Java 对象的变量<Something>.
				 * 然而，重要的是要意识到这些对象不像普通的 Java 对象列表或数组可以在本地迭代和操作。
				 * 您可以将这些对象视为对我们所需数据的引用或指针，因为这些数据可能分布在许多执行处理工作的机器上。
				 * 如果我们想正常处理这些数据，我们需要“收集”它，即要求 Spark 获取所有部分，无论它们在哪里，并将其转换为普通的 java 集合类型，如 List。
				 * 这就是 collectAsList() 方法的作用：
				 */	
				List<SteamGameStats> steamGamesList = steamGames.collectAsList();
				
				// steamGamesList 现在是一个我们可以分析的真正的 java 列表对象, e.g. find the 10 most popular games
				Collections.sort(steamGamesList); // SteamGameStats的默认排序是按推荐数排序，所以按这个排序
				Collections.reverse(steamGamesList); // Collections.sort()按升序排序，反过来得到推荐数的降序。
				
				
				spark.close(); // 关闭会话，现在我们已经完成了它。
				
				return steamGamesList;
	}
	
	/**
	 * 这是主方法，它将建立一个新的本地Spark会话，并运行示例应用程序以检查环境是否设置正确。
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
