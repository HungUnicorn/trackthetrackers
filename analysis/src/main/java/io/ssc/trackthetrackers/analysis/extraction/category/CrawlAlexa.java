package io.ssc.trackthetrackers.analysis.extraction.category;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.ssc.trackthetrackers.Config;

// Crawl Alexa category
public class CrawlAlexa {
	// Adult, Arts, Business, Computers, Games, Health, Home, Kids_and_Teens,
	// News, Recreation, Reference, Science, Shopping, Society, Sports, (World,
	// Regional are not needed)

	private static String argPathOut = Config.get("analysis.results.path")
			+ "SiteCategory" + "/";

	// top X (page 19th is top 500)
	// top x (page 20th is top 525, maximum)
	private static int numPges = 21;

	private static int pageDelay = 1;
	private static int categoryDelay = 3;

	public static HashSet<String> siteSet;
	public static ArrayList<String> urlList;
	public static AlexaTopCategorySite alexa;

	public static void main(String args[]) throws InterruptedException,
			IOException {
		// 15 categories
		String[] categories = { "Adult", "Arts", "Business", "Computers",
				"Games", "Health", "Home", "Kids_and_Teens", "News",
				"Recreation", "Reference", "Science", "Shopping", "Society",
				"Sports" };

		siteSet = new HashSet<String>();
		urlList = new ArrayList<String>();

		alexa = AlexaTopCategorySite.getInstance();
		getCategoryFromString(categories[0]);
	}

	public static void getCategoryFromString(String category)
			throws IOException, InterruptedException {

		urlList = generateUrl(category);
		for (String url : urlList) {
			siteSet.addAll(alexa.getSitesInCategory(url));
			// Wait X MILLISECONDS
			TimeUnit.SECONDS.sleep(pageDelay);
		}
		writeToDisk(argPathOut + category, siteSet, category);
	}

	// Was caught as a robot ( fail - HTTP error fetching URL. Status=403)
	public static void getCategoryFromArray(String[] categories)
			throws IOException, InterruptedException {
		for (String category : categories) {
			urlList = generateUrl(category);
			for (String url : urlList) {
				siteSet.addAll(alexa.getSitesInCategory(url));
				// Wait X MILLISECONDS
				TimeUnit.SECONDS.sleep(pageDelay);
			}
			writeToDisk(argPathOut + category, siteSet, category);
			TimeUnit.SECONDS.sleep(categoryDelay);
		}
	}

	public static ArrayList<String> generateUrl(String category) {
		// e.g.
		// url:http://www.alexa.com/topsites/category;1/Top/Games
		// url:http://www.alexa.com/topsites/category/Top/Games
		ArrayList<String> urlList = new ArrayList<String>();
		// url prefix
		String alexaUrl = "http://www.alexa.com/topsites/category/Top/";
		String alexaUrlFirstPage = alexaUrl + category;
		urlList.add(alexaUrlFirstPage);

		// generate url for each page
		for (int i = 1; i < numPges; i++) {
			// url prefix
			String alexaUrlRepeat = "http://www.alexa.com/topsites/category;";
			urlList.add(alexaUrlRepeat + i + "/Top/" + category);
		}
		System.out.println("generate urlList : " + category + " - "
				+ urlList.size());
		return urlList;

	}

	public static void writeToDisk(String filePath, Set<String> stringSet,
			String category) throws IOException {
		FileWriter fileWriter = new FileWriter(filePath);

		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		Iterator<String> iterator = stringSet.iterator();
		while (iterator.hasNext()) {
			String site = iterator.next();
			bufferedWriter.write(site + "," + category);
			bufferedWriter.newLine();
		}
		bufferedWriter.close();
		System.out.println("write: " + category);

	}
}
