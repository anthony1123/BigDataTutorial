package uk.ac.gla.dcs.bigdata.functions.map;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**
 * Extracts the MetaCritic score for a game (SteamGameStats object)
 * @author Richard
 *
 */
public class GameToMetaCriticScore implements MapFunction<SteamGameStats,Integer> {

	private static final long serialVersionUID = 525739182048149914L;

	@Override
	public Integer call(SteamGameStats value) throws Exception {
		return value.getMetacritic();
	}

}
