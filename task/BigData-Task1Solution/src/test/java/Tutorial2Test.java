package test.java;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import uk.ac.gla.dcs.bigdata.apps.SparkTutorial2a;
import uk.ac.gla.dcs.bigdata.apps.SparkTutorial2b;
import uk.ac.gla.dcs.bigdata.structures.SteamGameStats;

public class Tutorial2Test {

	public Tutorial2Test() {}
	
	@Test
	public void testTopGameRanking() throws Exception {
		
		
		SparkTutorial2a example2a = new SparkTutorial2a();
		
		List<SteamGameStats> steamGamesList = example2a.getRankSteamGames(false,false,true);
		for (SteamGameStats game : steamGamesList) {
			assertTrue(game.isPlatformmac()); // assert every game supports MacOS
		}
		
		SparkTutorial2b example2b = new SparkTutorial2b();
		
		double averageMetaCriticScore = example2b.getAverageMetaCriticScore(true,false,false); // pc=false, linux=false, mac=true
		
		assertEquals(averageMetaCriticScore, 54.282828282828284); // assert the correct value
		
	}
	
}
