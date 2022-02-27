package uk.ac.gla.dcs.bigdata.functions.flatmap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**
 *  这是一个FlatMapFunction的例子，在本例中，它根据Steam游戏所支持的平台，充当一个过滤器。
 *  该类的构造函数（最初在驱动程序上创建）存储了过滤器的选项，
 *  然后当为每个游戏调用flatmap时，调用方法将返回一个迭代器，
 *  如果游戏符合指定的平台，则返回一个迭代器，如果不符合则返回一个空迭代器。
 * @author Richard
 */
public class PlatformFilterFlatMap implements FlatMapFunction<SteamGameStats,SteamGameStats>{

	private static final long serialVersionUID = -5421918143346003481L;
	
	boolean supportsPC;
	boolean supportsLinux;
	boolean supportsMac;
	
	/**
	 * 默认构造函数，指定游戏必须支持的平台
	 * 不被过滤掉
	 * @param pc
	 * @param linux
	 * @param mac
	 */
	public PlatformFilterFlatMap(boolean pc, boolean linux, boolean mac) {
		this.supportsPC = pc;
		this.supportsLinux = linux;
		this.supportsMac = mac;
	}
	
	@Override
	public Iterator<SteamGameStats> call(SteamGameStats game) throws Exception {
		
		boolean matchesFilters = true;
		// 检查每个过滤选项，如果任何选项未通过检查，则设置 match=false
		if (supportsPC && !game.isPlatformwindows()) matchesFilters = false;
		if (supportsLinux && !game.isPlatformlinux()) matchesFilters = false;
		if (supportsMac && !game.isPlatformmac()) matchesFilters = false;
		
		if (matchesFilters) {
			// 游戏通过了我们所有的检查，所以返回它创建迭代器的方式是通过一个集合，例如一个列表
			List<SteamGameStats> gameList  = new ArrayList<SteamGameStats>(1); // 创建一个大小为 1 的空数组
			gameList.add(game); // 添加游戏
			return gameList.iterator(); // 返回列表的迭代器
		} else {
			// // 如果其中一项检查失败，不返回任何内容
			List<SteamGameStats> gameList  = new ArrayList<SteamGameStats>(0); // 创建一个大小为 0 的空数组
			return gameList.iterator(); // 返回空列表的迭代器

		}
	}
	
	

}
