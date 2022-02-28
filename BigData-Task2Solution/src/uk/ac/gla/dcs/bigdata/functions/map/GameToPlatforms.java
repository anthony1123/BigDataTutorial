package uk.ac.gla.dcs.bigdata.functions.map;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**
 * Converts a Game to a string specifying which platforms they support
 * @author Richard
 *
 */
public class GameToPlatforms implements MapFunction<SteamGameStats,String> {


	private static final long serialVersionUID = 8937787777009990893L;

	@Override
	public String call(SteamGameStats game) throws Exception {
		
		StringBuilder keyBuilder = new StringBuilder(); // we are going to build a unique string key to represent platforms
		
		if (game.isPlatformwindows()) keyBuilder.append("Win");
		if (game.isPlatformmac()) keyBuilder.append("Mac");
		if (game.isPlatformlinux()) keyBuilder.append("Lnx");
		
		
		return keyBuilder.toString();
	}




}
