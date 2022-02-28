package uk.ac.gla.dcs.bigdata.structures;

import java.io.Serializable;

/**
 * This is a class representing a statistics for a single steam game
 * @author Richard
 *
 */

public class SteamGameStats implements Serializable,Comparable<SteamGameStats> {

	private static final long serialVersionUID = -8091440707437715966L;
	
	int queryid;
	int responseid;
	String queryname;
	String responsename;
	String releasedate;
	int requiredage;
	int democount;
	int developercount;
	int dlccount;
	int metacritic;
	int moviecount;
	int packagecount;
	int recommendationcount;
	int publishercount;
	int screenshotcount;
	int steamspyowners;
	int steamspyownersvariance;
	int steamspyplayersestimate;
	int steamspyplayersvariance;
	int achievementcount;
	int achievementhighlightedcount;
	boolean controllersupport;
	boolean isfree;
	boolean freeveravail;
	boolean purchaseavail;
	boolean subscriptionavail;
	boolean platformwindows;
	boolean platformlinux;
	boolean platformmac;
	boolean pcreqshavemin;
	boolean pcreqshaverec;
	boolean linuxreqshavemin;
	boolean linuxreqshaverec;
	boolean macreqshavemin;
	boolean macreqshaverec;
	boolean categorysingleplayer;
	boolean categorymultiplayer;
	boolean categorycoop;
	boolean categorymmo;
	boolean categoryinapppurchase;
	boolean categoryincludesrcsdk;
	boolean categoryincludeleveleditor;
	boolean categoryvrsupport;
	boolean genreisnongame;
	boolean genreisindie;
	boolean genreisaction;
	boolean genreisadventure;
	boolean genreiscasual;
	boolean genreisstrategy;
	boolean genreisrpg;
	boolean genreissimulation;
	boolean genreisearlyaccess;
	boolean genreisfreetoplay;
	boolean genreissports;
	boolean genreisracing;
	boolean genreismassivelymultiplayer;
	String pricecurrency;
	double priceinitial;
	double pricefinal;
	String supportemail;
	String supporturl;
	String abouttext;
	String background;
	String shortdescrip;
	String detaileddescrip;
	String drmnotice;
	String extuseracctnotice;
	String headerimage;
	String legalnotice;
	String reviews;
	String supportedlanguages;
	String website;
	String pcminreqstext;
	String pcrecreqstext;
	String linuxminreqstext;
	String linuxrecreqstext;
	String macminreqstext;
	String macrecreqstext;
	
	public SteamGameStats() {}
	
	public SteamGameStats(int queryid, int responseid, String queryname, String responsename, String releasedate,
			int requiredage, int democount, int developercount, int dlccount, int metacritic, int moviecount,
			int packagecount, int recommendationcount, int publishercount, int screenshotcount, int steamspyowners,
			int steamspyownersvariance, int steamspyplayersestimate, int steamspyplayersvariance, int achievementcount,
			int achievementhighlightedcount, boolean controllersupport, boolean isfree, boolean freeveravail,
			boolean purchaseavail, boolean subscriptionavail, boolean platformwindows, boolean platformlinux,
			boolean platformmac, boolean pcreqshavemin, boolean pcreqshaverec, boolean linuxreqshavemin,
			boolean linuxreqshaverec, boolean macreqshavemin, boolean macreqshaverec, boolean categorysingleplayer,
			boolean categorymultiplayer, boolean categorycoop, boolean categorymmo, boolean categoryinapppurchase,
			boolean categoryincludesrcsdk, boolean categoryincludeleveleditor, boolean categoryvrsupport,
			boolean genreisnongame, boolean genreisindie, boolean genreisaction, boolean genreisadventure,
			boolean genreiscasual, boolean genreisstrategy, boolean genreisrpg, boolean genreissimulation,
			boolean genreisearlyaccess, boolean genreisfreetoplay, boolean genreissports, boolean genreisracing,
			boolean genreismassivelymultiplayer, String pricecurrency, double priceinitial, double pricefinal,
			String supportemail, String supporturl, String abouttext, String background, String shortdescrip,
			String detaileddescrip, String drmnotice, String extuseracctnotice, String headerimage, String legalnotice,
			String reviews, String supportedlanguages, String website, String pcminreqstext, String pcrecreqstext,
			String linuxminreqstext, String linuxrecreqstext, String macminreqstext, String macrecreqstext) {
		super();
		this.queryid = queryid;
		this.responseid = responseid;
		this.queryname = queryname;
		this.responsename = responsename;
		this.releasedate = releasedate;
		this.requiredage = requiredage;
		this.democount = democount;
		this.developercount = developercount;
		this.dlccount = dlccount;
		this.metacritic = metacritic;
		this.moviecount = moviecount;
		this.packagecount = packagecount;
		this.recommendationcount = recommendationcount;
		this.publishercount = publishercount;
		this.screenshotcount = screenshotcount;
		this.steamspyowners = steamspyowners;
		this.steamspyownersvariance = steamspyownersvariance;
		this.steamspyplayersestimate = steamspyplayersestimate;
		this.steamspyplayersvariance = steamspyplayersvariance;
		this.achievementcount = achievementcount;
		this.achievementhighlightedcount = achievementhighlightedcount;
		this.controllersupport = controllersupport;
		this.isfree = isfree;
		this.freeveravail = freeveravail;
		this.purchaseavail = purchaseavail;
		this.subscriptionavail = subscriptionavail;
		this.platformwindows = platformwindows;
		this.platformlinux = platformlinux;
		this.platformmac = platformmac;
		this.pcreqshavemin = pcreqshavemin;
		this.pcreqshaverec = pcreqshaverec;
		this.linuxreqshavemin = linuxreqshavemin;
		this.linuxreqshaverec = linuxreqshaverec;
		this.macreqshavemin = macreqshavemin;
		this.macreqshaverec = macreqshaverec;
		this.categorysingleplayer = categorysingleplayer;
		this.categorymultiplayer = categorymultiplayer;
		this.categorycoop = categorycoop;
		this.categorymmo = categorymmo;
		this.categoryinapppurchase = categoryinapppurchase;
		this.categoryincludesrcsdk = categoryincludesrcsdk;
		this.categoryincludeleveleditor = categoryincludeleveleditor;
		this.categoryvrsupport = categoryvrsupport;
		this.genreisnongame = genreisnongame;
		this.genreisindie = genreisindie;
		this.genreisaction = genreisaction;
		this.genreisadventure = genreisadventure;
		this.genreiscasual = genreiscasual;
		this.genreisstrategy = genreisstrategy;
		this.genreisrpg = genreisrpg;
		this.genreissimulation = genreissimulation;
		this.genreisearlyaccess = genreisearlyaccess;
		this.genreisfreetoplay = genreisfreetoplay;
		this.genreissports = genreissports;
		this.genreisracing = genreisracing;
		this.genreismassivelymultiplayer = genreismassivelymultiplayer;
		this.pricecurrency = pricecurrency;
		this.priceinitial = priceinitial;
		this.pricefinal = pricefinal;
		this.supportemail = supportemail;
		this.supporturl = supporturl;
		this.abouttext = abouttext;
		this.background = background;
		this.shortdescrip = shortdescrip;
		this.detaileddescrip = detaileddescrip;
		this.drmnotice = drmnotice;
		this.extuseracctnotice = extuseracctnotice;
		this.headerimage = headerimage;
		this.legalnotice = legalnotice;
		this.reviews = reviews;
		this.supportedlanguages = supportedlanguages;
		this.website = website;
		this.pcminreqstext = pcminreqstext;
		this.pcrecreqstext = pcrecreqstext;
		this.linuxminreqstext = linuxminreqstext;
		this.linuxrecreqstext = linuxrecreqstext;
		this.macminreqstext = macminreqstext;
		this.macrecreqstext = macrecreqstext;
	}

	public int getQueryid() {
		return queryid;
	}

	public void setQueryid(int queryid) {
		this.queryid = queryid;
	}

	public int getResponseid() {
		return responseid;
	}

	public void setResponseid(int responseid) {
		this.responseid = responseid;
	}

	public String getQueryname() {
		return queryname;
	}

	public void setQueryname(String queryname) {
		this.queryname = queryname;
	}

	public String getResponsename() {
		return responsename;
	}

	public void setResponsename(String responsename) {
		this.responsename = responsename;
	}

	public String getReleasedate() {
		return releasedate;
	}

	public void setReleasedate(String releasedate) {
		this.releasedate = releasedate;
	}

	public int getRequiredage() {
		return requiredage;
	}

	public void setRequiredage(int requiredage) {
		this.requiredage = requiredage;
	}

	public int getDemocount() {
		return democount;
	}

	public void setDemocount(int democount) {
		this.democount = democount;
	}

	public int getDevelopercount() {
		return developercount;
	}

	public void setDevelopercount(int developercount) {
		this.developercount = developercount;
	}

	public int getDlccount() {
		return dlccount;
	}

	public void setDlccount(int dlccount) {
		this.dlccount = dlccount;
	}

	public int getMetacritic() {
		return metacritic;
	}

	public void setMetacritic(int metacritic) {
		this.metacritic = metacritic;
	}

	public int getMoviecount() {
		return moviecount;
	}

	public void setMoviecount(int moviecount) {
		this.moviecount = moviecount;
	}

	public int getPackagecount() {
		return packagecount;
	}

	public void setPackagecount(int packagecount) {
		this.packagecount = packagecount;
	}

	public int getRecommendationcount() {
		return recommendationcount;
	}

	public void setRecommendationcount(int recommendationcount) {
		this.recommendationcount = recommendationcount;
	}

	public int getPublishercount() {
		return publishercount;
	}

	public void setPublishercount(int publishercount) {
		this.publishercount = publishercount;
	}

	public int getScreenshotcount() {
		return screenshotcount;
	}

	public void setScreenshotcount(int screenshotcount) {
		this.screenshotcount = screenshotcount;
	}

	public int getSteamspyowners() {
		return steamspyowners;
	}

	public void setSteamspyowners(int steamspyowners) {
		this.steamspyowners = steamspyowners;
	}

	public int getSteamspyownersvariance() {
		return steamspyownersvariance;
	}

	public void setSteamspyownersvariance(int steamspyownersvariance) {
		this.steamspyownersvariance = steamspyownersvariance;
	}

	public int getSteamspyplayersestimate() {
		return steamspyplayersestimate;
	}

	public void setSteamspyplayersestimate(int steamspyplayersestimate) {
		this.steamspyplayersestimate = steamspyplayersestimate;
	}

	public int getSteamspyplayersvariance() {
		return steamspyplayersvariance;
	}

	public void setSteamspyplayersvariance(int steamspyplayersvariance) {
		this.steamspyplayersvariance = steamspyplayersvariance;
	}

	public int getAchievementcount() {
		return achievementcount;
	}

	public void setAchievementcount(int achievementcount) {
		this.achievementcount = achievementcount;
	}

	public int getAchievementhighlightedcount() {
		return achievementhighlightedcount;
	}

	public void setAchievementhighlightedcount(int achievementhighlightedcount) {
		this.achievementhighlightedcount = achievementhighlightedcount;
	}

	public boolean isControllersupport() {
		return controllersupport;
	}

	public void setControllersupport(boolean controllersupport) {
		this.controllersupport = controllersupport;
	}

	public boolean isIsfree() {
		return isfree;
	}

	public void setIsfree(boolean isfree) {
		this.isfree = isfree;
	}

	public boolean isFreeveravail() {
		return freeveravail;
	}

	public void setFreeveravail(boolean freeveravail) {
		this.freeveravail = freeveravail;
	}

	public boolean isPurchaseavail() {
		return purchaseavail;
	}

	public void setPurchaseavail(boolean purchaseavail) {
		this.purchaseavail = purchaseavail;
	}

	public boolean isSubscriptionavail() {
		return subscriptionavail;
	}

	public void setSubscriptionavail(boolean subscriptionavail) {
		this.subscriptionavail = subscriptionavail;
	}

	public boolean isPlatformwindows() {
		return platformwindows;
	}

	public void setPlatformwindows(boolean platformwindows) {
		this.platformwindows = platformwindows;
	}

	public boolean isPlatformlinux() {
		return platformlinux;
	}

	public void setPlatformlinux(boolean platformlinux) {
		this.platformlinux = platformlinux;
	}

	public boolean isPlatformmac() {
		return platformmac;
	}

	public void setPlatformmac(boolean platformmac) {
		this.platformmac = platformmac;
	}

	public boolean isPcreqshavemin() {
		return pcreqshavemin;
	}

	public void setPcreqshavemin(boolean pcreqshavemin) {
		this.pcreqshavemin = pcreqshavemin;
	}

	public boolean isPcreqshaverec() {
		return pcreqshaverec;
	}

	public void setPcreqshaverec(boolean pcreqshaverec) {
		this.pcreqshaverec = pcreqshaverec;
	}

	public boolean isLinuxreqshavemin() {
		return linuxreqshavemin;
	}

	public void setLinuxreqshavemin(boolean linuxreqshavemin) {
		this.linuxreqshavemin = linuxreqshavemin;
	}

	public boolean isLinuxreqshaverec() {
		return linuxreqshaverec;
	}

	public void setLinuxreqshaverec(boolean linuxreqshaverec) {
		this.linuxreqshaverec = linuxreqshaverec;
	}

	public boolean isMacreqshavemin() {
		return macreqshavemin;
	}

	public void setMacreqshavemin(boolean macreqshavemin) {
		this.macreqshavemin = macreqshavemin;
	}

	public boolean isMacreqshaverec() {
		return macreqshaverec;
	}

	public void setMacreqshaverec(boolean macreqshaverec) {
		this.macreqshaverec = macreqshaverec;
	}

	public boolean isCategorysingleplayer() {
		return categorysingleplayer;
	}

	public void setCategorysingleplayer(boolean categorysingleplayer) {
		this.categorysingleplayer = categorysingleplayer;
	}

	public boolean isCategorymultiplayer() {
		return categorymultiplayer;
	}

	public void setCategorymultiplayer(boolean categorymultiplayer) {
		this.categorymultiplayer = categorymultiplayer;
	}

	public boolean isCategorycoop() {
		return categorycoop;
	}

	public void setCategorycoop(boolean categorycoop) {
		this.categorycoop = categorycoop;
	}

	public boolean isCategorymmo() {
		return categorymmo;
	}

	public void setCategorymmo(boolean categorymmo) {
		this.categorymmo = categorymmo;
	}

	public boolean isCategoryinapppurchase() {
		return categoryinapppurchase;
	}

	public void setCategoryinapppurchase(boolean categoryinapppurchase) {
		this.categoryinapppurchase = categoryinapppurchase;
	}

	public boolean isCategoryincludesrcsdk() {
		return categoryincludesrcsdk;
	}

	public void setCategoryincludesrcsdk(boolean categoryincludesrcsdk) {
		this.categoryincludesrcsdk = categoryincludesrcsdk;
	}

	public boolean isCategoryincludeleveleditor() {
		return categoryincludeleveleditor;
	}

	public void setCategoryincludeleveleditor(boolean categoryincludeleveleditor) {
		this.categoryincludeleveleditor = categoryincludeleveleditor;
	}

	public boolean isCategoryvrsupport() {
		return categoryvrsupport;
	}

	public void setCategoryvrsupport(boolean categoryvrsupport) {
		this.categoryvrsupport = categoryvrsupport;
	}

	public boolean isGenreisnongame() {
		return genreisnongame;
	}

	public void setGenreisnongame(boolean genreisnongame) {
		this.genreisnongame = genreisnongame;
	}

	public boolean isGenreisindie() {
		return genreisindie;
	}

	public void setGenreisindie(boolean genreisindie) {
		this.genreisindie = genreisindie;
	}

	public boolean isGenreisaction() {
		return genreisaction;
	}

	public void setGenreisaction(boolean genreisaction) {
		this.genreisaction = genreisaction;
	}

	public boolean isGenreisadventure() {
		return genreisadventure;
	}

	public void setGenreisadventure(boolean genreisadventure) {
		this.genreisadventure = genreisadventure;
	}

	public boolean isGenreiscasual() {
		return genreiscasual;
	}

	public void setGenreiscasual(boolean genreiscasual) {
		this.genreiscasual = genreiscasual;
	}

	public boolean isGenreisstrategy() {
		return genreisstrategy;
	}

	public void setGenreisstrategy(boolean genreisstrategy) {
		this.genreisstrategy = genreisstrategy;
	}

	public boolean isGenreisrpg() {
		return genreisrpg;
	}

	public void setGenreisrpg(boolean genreisrpg) {
		this.genreisrpg = genreisrpg;
	}

	public boolean isGenreissimulation() {
		return genreissimulation;
	}

	public void setGenreissimulation(boolean genreissimulation) {
		this.genreissimulation = genreissimulation;
	}

	public boolean isGenreisearlyaccess() {
		return genreisearlyaccess;
	}

	public void setGenreisearlyaccess(boolean genreisearlyaccess) {
		this.genreisearlyaccess = genreisearlyaccess;
	}

	public boolean isGenreisfreetoplay() {
		return genreisfreetoplay;
	}

	public void setGenreisfreetoplay(boolean genreisfreetoplay) {
		this.genreisfreetoplay = genreisfreetoplay;
	}

	public boolean isGenreissports() {
		return genreissports;
	}

	public void setGenreissports(boolean genreissports) {
		this.genreissports = genreissports;
	}

	public boolean isGenreisracing() {
		return genreisracing;
	}

	public void setGenreisracing(boolean genreisracing) {
		this.genreisracing = genreisracing;
	}

	public boolean isGenreismassivelymultiplayer() {
		return genreismassivelymultiplayer;
	}

	public void setGenreismassivelymultiplayer(boolean genreismassivelymultiplayer) {
		this.genreismassivelymultiplayer = genreismassivelymultiplayer;
	}

	public String getPricecurrency() {
		return pricecurrency;
	}

	public void setPricecurrency(String pricecurrency) {
		this.pricecurrency = pricecurrency;
	}

	public double getPriceinitial() {
		return priceinitial;
	}

	public void setPriceinitial(double priceinitial) {
		this.priceinitial = priceinitial;
	}

	public double getPricefinal() {
		return pricefinal;
	}

	public void setPricefinal(double pricefinal) {
		this.pricefinal = pricefinal;
	}

	public String getSupportemail() {
		return supportemail;
	}

	public void setSupportemail(String supportemail) {
		this.supportemail = supportemail;
	}

	public String getSupporturl() {
		return supporturl;
	}

	public void setSupporturl(String supporturl) {
		this.supporturl = supporturl;
	}

	public String getAbouttext() {
		return abouttext;
	}

	public void setAbouttext(String abouttext) {
		this.abouttext = abouttext;
	}

	public String getBackground() {
		return background;
	}

	public void setBackground(String background) {
		this.background = background;
	}

	public String getShortdescrip() {
		return shortdescrip;
	}

	public void setShortdescrip(String shortdescrip) {
		this.shortdescrip = shortdescrip;
	}

	public String getDetaileddescrip() {
		return detaileddescrip;
	}

	public void setDetaileddescrip(String detaileddescrip) {
		this.detaileddescrip = detaileddescrip;
	}

	public String getDrmnotice() {
		return drmnotice;
	}

	public void setDrmnotice(String drmnotice) {
		this.drmnotice = drmnotice;
	}

	public String getExtuseracctnotice() {
		return extuseracctnotice;
	}

	public void setExtuseracctnotice(String extuseracctnotice) {
		this.extuseracctnotice = extuseracctnotice;
	}

	public String getHeaderimage() {
		return headerimage;
	}

	public void setHeaderimage(String headerimage) {
		this.headerimage = headerimage;
	}

	public String getLegalnotice() {
		return legalnotice;
	}

	public void setLegalnotice(String legalnotice) {
		this.legalnotice = legalnotice;
	}

	public String getReviews() {
		return reviews;
	}

	public void setReviews(String reviews) {
		this.reviews = reviews;
	}

	public String getSupportedlanguages() {
		return supportedlanguages;
	}

	public void setSupportedlanguages(String supportedlanguages) {
		this.supportedlanguages = supportedlanguages;
	}

	public String getWebsite() {
		return website;
	}

	public void setWebsite(String website) {
		this.website = website;
	}

	public String getPcminreqstext() {
		return pcminreqstext;
	}

	public void setPcminreqstext(String pcminreqstext) {
		this.pcminreqstext = pcminreqstext;
	}

	public String getPcrecreqstext() {
		return pcrecreqstext;
	}

	public void setPcrecreqstext(String pcrecreqstext) {
		this.pcrecreqstext = pcrecreqstext;
	}

	public String getLinuxminreqstext() {
		return linuxminreqstext;
	}

	public void setLinuxminreqstext(String linuxminreqstext) {
		this.linuxminreqstext = linuxminreqstext;
	}

	public String getLinuxrecreqstext() {
		return linuxrecreqstext;
	}

	public void setLinuxrecreqstext(String linuxrecreqstext) {
		this.linuxrecreqstext = linuxrecreqstext;
	}

	public String getMacminreqstext() {
		return macminreqstext;
	}

	public void setMacminreqstext(String macminreqstext) {
		this.macminreqstext = macminreqstext;
	}

	public String getMacrecreqstext() {
		return macrecreqstext;
	}

	public void setMacrecreqstext(String macrecreqstext) {
		this.macrecreqstext = macrecreqstext;
	}
	
	public String getTitle() {
		return this.responsename;
	}

	
	@SuppressWarnings("deprecation")
	@Override
	public int compareTo(SteamGameStats o) {
		return new Integer(recommendationcount).compareTo(o.recommendationcount);
	}
	
	
	

	
}
