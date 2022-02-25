package uk.ac.gla.dcs.bigdata.functions.map;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**
 * A very simple map function, just extracts the metacritic score for a steam game
 * and returns it
 * @author Richard
 *
 */
public class SteamGameToMetaCriticScoreMap implements MapFunction<SteamGameStats,Integer>{

	private static final long serialVersionUID = -4771496746335862000L;

	@Override
	public Integer call(SteamGameStats value) throws Exception {
		return value.getMetacritic();
	}

}
