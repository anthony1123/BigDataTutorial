package uk.ac.gla.dcs.bigdata.functions.flatmap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

public class RequirementsFilter implements FlatMapFunction<SteamGameStats,SteamGameStats> {

	private static final long serialVersionUID = 8797501453976545434L;

	@Override
	public Iterator<SteamGameStats> call(SteamGameStats game) throws Exception {

		String windowsRequirements = game.getPcminreqstext();
		
		if (windowsRequirements==null) {
			List<SteamGameStats> gameList  = new ArrayList<SteamGameStats>(0); // create an empty array of size 1
			return gameList.iterator(); // return the iterator for the list
		}
		
		if (windowsRequirements.toLowerCase().contains("windows xp")) {
			List<SteamGameStats> gameList  = new ArrayList<SteamGameStats>(1); // create an empty array of size 1
			gameList.add(game); // add the game
			return gameList.iterator(); // return the iterator for the list
		} else {
			List<SteamGameStats> gameList  = new ArrayList<SteamGameStats>(0); // create an empty array of size 1
			return gameList.iterator(); // return the iterator for the list
		}
	
	}
	
	
	
	
	

}
