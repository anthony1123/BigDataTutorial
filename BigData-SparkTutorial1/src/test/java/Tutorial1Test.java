package test.java;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import uk.ac.gla.dcs.bigdata.apps.SparkTutorial1;
import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

public class Tutorial1Test {

	public Tutorial1Test() {}
	
	@Test
	public void testTopGameRanking() throws Exception {
		
		
		SparkTutorial1 example1 = new SparkTutorial1();
		
		List<SteamGameStats> steamGamesList = example1.getRankSteamGames();
		
		assertEquals(steamGamesList.get(0).getTitle(), "Counter-Strike: Global Offensive");
		assertEquals(steamGamesList.get(1).getTitle(), "Dota 2");
		assertEquals(steamGamesList.get(2).getTitle(), "Team Fortress 2");
		
	}
	
}
