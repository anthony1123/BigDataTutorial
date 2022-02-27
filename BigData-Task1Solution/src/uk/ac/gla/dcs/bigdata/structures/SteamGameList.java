package uk.ac.gla.dcs.bigdata.structures;

import java.io.Serializable;
import java.util.List;

/**
 * This is a Java class representing a list of Steam games. This is used
 * as a wrapper for serialisation, since Encoders cannot take lists as input.
 * @author Richard
 *
 */
public class SteamGameList implements Serializable{

	private static final long serialVersionUID = -5066661179554881886L;
	List<SteamGameStats> gameList;

	public SteamGameList() {}
	
	public SteamGameList(List<SteamGameStats> gameList) {
		super();
		this.gameList = gameList;
	}

	public List<SteamGameStats> getGameList() {
		return gameList;
	}

	public void setGameList(List<SteamGameStats> gameList) {
		this.gameList = gameList;
	}
	
	
	
}
