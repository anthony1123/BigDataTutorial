package uk.ac.gla.dcs.bigdata.functions.flatmapgroups;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

public class FilterByPriceAndCountWords implements FlatMapGroupsFunction<String,SteamGameStats,SteamGameStats>{

	
	private static final long serialVersionUID = -484810270146328326L;
	
	// Counters/Accumulators
	LongAccumulator wordCountAccumulator;
	LongAccumulator docCountAccumulator;
	
	// Global Data
	Broadcast<Set<String>> broadcastStopwords;
	
	// Minimum Price
	double minPrice;
	
	public FilterByPriceAndCountWords(LongAccumulator wordCountAccumulator, LongAccumulator docCountAccumulator, Broadcast<Set<String>> broadcastStopwords, double minPrice) {
		this.wordCountAccumulator =wordCountAccumulator;
		this.docCountAccumulator = docCountAccumulator;
		this.broadcastStopwords =broadcastStopwords;
		this.minPrice = minPrice;
		
	}
	
	@Override
	public Iterator<SteamGameStats> call(String key, Iterator<SteamGameStats> games) throws Exception {
		
		List<SteamGameStats> filteredGamesByPrice = new ArrayList<SteamGameStats>();
		
		// get the list of words
		Set<String> stopwords = broadcastStopwords.value();
		
		boolean supportsMac = key.contains("Mac");
		
		while (games.hasNext()) {
			SteamGameStats game = games.next();
			
			if (game.getPriceinitial()>=minPrice) {
				
				filteredGamesByPrice.add(game);
				
				// now lets calculate the extra statistics we wanted
				String description = game.getDetaileddescrip();
				if (description!=null) {
					
					docCountAccumulator.add(1);
					
					for (String word : description.split(" ")) { // split string on space character
						if (!stopwords.contains(word.toLowerCase())) { // if word is not a stopword
							
							if (supportsMac) wordCountAccumulator.add(1);
							
						}
					}
					
				}
				
			}
			 
		}
		
		return filteredGamesByPrice.iterator();
	}

}
