package uk.ac.gla.dcs.bigdata.functions.flatmap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

/**
 * This is an example of a FlatMapFunction, which in this case acts as a filter for
 * Steam games based on what platforms they support. The constructor of the class
 * (initially created on the driver program) stores what the filter options are, then
 * when flatmap is called for each game, the call method will either return an iterator
 * with the game if it matches the specified platforms, or an empty iterator if not.
 * @author Richard
 *
 */
public class PlatformFilterFlatMap implements FlatMapFunction<SteamGameStats,SteamGameStats>{

	private static final long serialVersionUID = -5421918143346003481L;
	
	boolean supportsPC;
	boolean supportsLinux;
	boolean supportsMac;
	
	/**
	 * Default constructor, specifies the platforms that the game must support
	 * not to be filtered out
	 * @param pc
	 * @param linux
	 * @param mac
	 */
	public PlatformFilterFlatMap(boolean pc, boolean linux, boolean mac) {
		this.supportsPC = pc;
		this.supportsLinux = linux;
		this.supportsMac = mac;
	}
	
	@Override
	public Iterator<SteamGameStats> call(SteamGameStats game) throws Exception {
		
		boolean matchesFilters = true;
		
		// check each filtering option, set match=false if any options fail the check
		if (supportsPC && !game.isPlatformwindows()) matchesFilters = false;
		if (supportsLinux && !game.isPlatformlinux()) matchesFilters = false;
		if (supportsMac && !game.isPlatformmac()) matchesFilters = false;
		
		if (matchesFilters) {
			// the game passed all our checks, so return it
			// the way to create an iterator is via a collection, e.g. a list
			List<SteamGameStats> gameList  = new ArrayList<SteamGameStats>(1); // create an empty array of size 1
			gameList.add(game); // add the game
			return gameList.iterator(); // return the iterator for the list
		} else {
			// if one of the check fails we want to return nothing
			List<SteamGameStats> gameList  = new ArrayList<SteamGameStats>(0); // create an empty array of size 0
			return gameList.iterator(); // return the iterator for the empty list

		}
	}
	
	

}
