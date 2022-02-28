package uk.ac.gla.dcs.bigdata.functions.mapgroups;

import java.util.Iterator;

import org.apache.spark.api.java.function.MapGroupsFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**
 * MapGroups function that calculates the average price for all games given a key
 * @author Richard
 *
 */
public class AVGPrice implements MapGroupsFunction<Integer, SteamGameStats, Tuple2<Integer,Double>>{
	
	private static final long serialVersionUID = -6456363146611418557L;

	@Override
	public Tuple2<Integer, Double> call(Integer key, Iterator<SteamGameStats> values) throws Exception {
		
		double averagePrice = 0.0;

		int count = 0;
		while (values.hasNext()) {
			SteamGameStats game = values.next();
			averagePrice=averagePrice+game.getPriceinitial();
			count++;
		}
		
		averagePrice = averagePrice/count;
		
		
		return new Tuple2<Integer,Double>(key,averagePrice);
	}

}
