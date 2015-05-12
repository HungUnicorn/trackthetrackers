package io.ssc.trackthetrackers.analysis.publisher.category;

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
	// News, Recreation, Reference, Regional, Science, Shopping, Society, Sports

	//private static ArrayList<String> categoryList;
	private static String category = "Kids_and_Teens";

	private static String argPathOut = Config.get("analysis.results.path")
			+ "SiteCategory" + "/" + category;

	// top X (page 19th is top 500)
	private static int numPges = 19;

	private static int delay = 1;

	public static void main(String args[]) throws IOException,
			InterruptedException {

		AlexaTopCategorySite alexa = AlexaTopCategorySite.getInstance();

		HashSet<String> siteSet = new HashSet<String>();
		ArrayList<String> urlList = new ArrayList<String>();
		urlList = generateUrl(category);

		for (String url : urlList) {
			siteSet.addAll(alexa.getSitesInCategory(url));
			// Wait X MILLISECONDS
			TimeUnit.SECONDS.sleep(delay);

		}
		writeToDisk(argPathOut, siteSet, category);
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
		System.out.println("generate urlList :" + urlList.size());
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
		System.out.println("write");

	}
}
