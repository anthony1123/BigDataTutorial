package uk.ac.gla.dcs.bigdata.functions.map;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.structures.SteamGameList;
import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**
 * Simply converts a steam game to a list of steam games, to make it in an appropriate form
 * for doing reduce-based filtering operations
 * @author Richard
 *
 */
public class SteamGamesToListMap implements MapFunction<SteamGameStats,SteamGameList> {

	private static final long serialVersionUID = 1L;

	@Override
	public SteamGameList call(SteamGameStats game) throws Exception {
		List<SteamGameStats> asList = new ArrayList<SteamGameStats>(1);
		asList.add(game);
		return new SteamGameList(asList);
	}

}
