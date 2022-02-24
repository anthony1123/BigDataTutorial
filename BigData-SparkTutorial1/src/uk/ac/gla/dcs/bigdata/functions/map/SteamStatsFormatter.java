package uk.ac.gla.dcs.bigdata.functions.map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**

 *这个map函数将steam数据集中每一行转换为一个SteamGameStats对象，
 */

public class SteamStatsFormatter implements MapFunction<Row,SteamGameStats> {

	private static final long serialVersionUID = 1202272537118936631L;

	@Override
	/**这是为每个输入行调用的方法，将其转换为SteamGameStats 对象；
	 * 在这种情况下，假定Rows已被 * spark.read ().csv() 读取，它不会推断 * csv 文件中每一列的数据类型，仅将每个条目视为字符串，
	 * 因此我们需要在这里转换为整数和双精度数
	 */
	
	
	public SteamGameStats call(Row value) throws Exception {
		
		
		SteamGameStats gameStats = new SteamGameStats(
				Integer.parseInt(value.getString(0)),
				Integer.parseInt(value.getString(1)),
				value.getString(2),
				value.getString(3),
				value.getString(4),
				Integer.parseInt(value.getString(5)),
				Integer.parseInt(value.getString(6)),
				Integer.parseInt(value.getString(7)),
				Integer.parseInt(value.getString(8)),
				Integer.parseInt(value.getString(9)),
				Integer.parseInt(value.getString(10)),
				Integer.parseInt(value.getString(11)),
				Integer.parseInt(value.getString(12)),
				Integer.parseInt(value.getString(13)),
				Integer.parseInt(value.getString(14)),
				Integer.parseInt(value.getString(15)),
				Integer.parseInt(value.getString(16)),
				Integer.parseInt(value.getString(17)),
				Integer.parseInt(value.getString(18)),
				Integer.parseInt(value.getString(19)),
				Integer.parseInt(value.getString(20)),
				Boolean.parseBoolean(value.getString(21)),
				Boolean.parseBoolean(value.getString(22)),
				Boolean.parseBoolean(value.getString(23)),
				Boolean.parseBoolean(value.getString(24)),
				Boolean.parseBoolean(value.getString(25)),
				Boolean.parseBoolean(value.getString(26)),
				Boolean.parseBoolean(value.getString(27)),
				Boolean.parseBoolean(value.getString(28)),
				Boolean.parseBoolean(value.getString(29)),
				Boolean.parseBoolean(value.getString(30)),
				Boolean.parseBoolean(value.getString(31)),
				Boolean.parseBoolean(value.getString(32)),
				Boolean.parseBoolean(value.getString(33)),
				Boolean.parseBoolean(value.getString(34)),
				Boolean.parseBoolean(value.getString(35)),
				Boolean.parseBoolean(value.getString(36)),
				Boolean.parseBoolean(value.getString(37)),
				Boolean.parseBoolean(value.getString(38)),
				Boolean.parseBoolean(value.getString(39)),
				Boolean.parseBoolean(value.getString(40)),
				Boolean.parseBoolean(value.getString(41)),
				Boolean.parseBoolean(value.getString(42)),
				Boolean.parseBoolean(value.getString(43)),
				Boolean.parseBoolean(value.getString(44)),
				Boolean.parseBoolean(value.getString(45)),
				Boolean.parseBoolean(value.getString(46)),
				Boolean.parseBoolean(value.getString(47)),
				Boolean.parseBoolean(value.getString(48)),
				Boolean.parseBoolean(value.getString(49)),
				Boolean.parseBoolean(value.getString(50)),
				Boolean.parseBoolean(value.getString(51)),
				Boolean.parseBoolean(value.getString(52)),
				Boolean.parseBoolean(value.getString(53)),
				Boolean.parseBoolean(value.getString(54)),
				Boolean.parseBoolean(value.getString(55)),
				value.getString(56),
				Double.parseDouble(value.getString(57)),
				Double.parseDouble(value.getString(58)),
				value.getString(59),
				value.getString(60),
				value.getString(61),
				value.getString(62),
				value.getString(63),
				value.getString(64),
				value.getString(65),
				value.getString(66),
				value.getString(67),
				value.getString(68),
				value.getString(69),
				value.getString(70),
				value.getString(71),
				value.getString(72),
				value.getString(73),
				value.getString(74),
				value.getString(75),
				value.getString(76),
				value.getString(77)
				);
		
		
		return gameStats;
	}

}
