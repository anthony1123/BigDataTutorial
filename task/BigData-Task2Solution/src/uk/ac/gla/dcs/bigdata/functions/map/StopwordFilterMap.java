package uk.ac.gla.dcs.bigdata.functions.map;

import java.util.Set;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**
 * Removes stopwords from the detailed description field of a Steam Game
 * @author Richard
 *
 */
public class StopwordFilterMap implements MapFunction<SteamGameStats, SteamGameStats>{


	private static final long serialVersionUID = 5685602376695019352L;
	
	// Global Data
	Broadcast<Set<String>> broadcastStopwords;
	
	// Extra Data Accumulators
	LongAccumulator coopCountAccumulator;
	LongAccumulator storyCountAccumulator;
	
	public StopwordFilterMap(Broadcast<Set<String>> broadcastStopwords, LongAccumulator coopCountAccumulator, LongAccumulator storyCountAccumulator) {
		this.broadcastStopwords = broadcastStopwords;
		this.coopCountAccumulator= coopCountAccumulator;
		this.storyCountAccumulator = storyCountAccumulator;
	}
	
	@Override
	public SteamGameStats call(SteamGameStats game) throws Exception {

		// get the list of words
		Set<String> stopwords = broadcastStopwords.value();
		
		// We don't want to add one to the count of the accumulators for each game if they
		// match the criteria, so we use a boolean value during parsing and then check if
		// any matches were found afterward.
		boolean coopFound = false;
		boolean storyFound = false;
		
		// now lets filter out stopwrds from the game description
		String description = game.getDetaileddescrip();
		if (description!=null) {
			
			StringBuilder stringBuilder = new StringBuilder();
			
			for (String word : description.split(" ")) { // split string on space character
				if (!stopwords.contains(word.toLowerCase())) { // if word is not a stopword
					
					stringBuilder.append(word); // if not a stopword, append to our new description
					stringBuilder.append(" ");
					
				}
				
				
				if (word.equalsIgnoreCase("co-op")) coopFound=true;
				if (word.equalsIgnoreCase("story")) storyFound=true;
			}
			
			if (coopFound) coopCountAccumulator.add(1);
			if (storyFound) storyCountAccumulator.add(1);
			
			game.setDetaileddescrip(stringBuilder.toString()); // update the game object
			
		}
		
		return game;
	}
	
	
	

}
