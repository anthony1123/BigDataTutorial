package uk.ac.gla.dcs.bigdata.functions.map;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**
 * Converts SteamGameStats objects to prices
 * @author Richard
 *
 */
public class PriceMap implements MapFunction<SteamGameStats,Double>{

	private static final long serialVersionUID = 7670091056565802697L;

	@Override
	public Double call(SteamGameStats value) throws Exception {
		return value.getPriceinitial();
	}

}
