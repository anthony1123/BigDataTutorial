package uk.ac.gla.dcs.bigdata.functions.reducer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.structures.SteamGameList;
import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

public class SameTitleReducer implements ReduceFunction<SteamGameList> {

	private static final long serialVersionUID = -2234797120289678538L;

	@Override
	public SteamGameList call(SteamGameList v1, SteamGameList v2) throws Exception {
		
		// Use a fast method to find String matches using hash map
		Map<String,SteamGameStats> titleGameMap = new HashMap<String,SteamGameStats>();
		
		// If titles are identical, the hash map entry will overwrite the existing one, meaning we will be left with one game
		// instance for each unique title
		for (SteamGameStats game : v1.getGameList()) titleGameMap.put(game.getTitle(), game);
		for (SteamGameStats game : v2.getGameList()) titleGameMap.put(game.getTitle(), game);
		 
		// create a list in which we will store games with unique titles
		List<SteamGameStats> uniqueGames = new ArrayList<SteamGameStats>(v1.getGameList().size()+v2.getGameList().size());
		uniqueGames.addAll(titleGameMap.values()); // add all the games with unique titles to our array
		
		// convert to SteamGameList and return
		return new SteamGameList(uniqueGames);
	}



}
