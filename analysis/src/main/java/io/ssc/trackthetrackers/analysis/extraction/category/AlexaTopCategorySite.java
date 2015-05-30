package io.ssc.trackthetrackers.analysis.extraction.category;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.flink.shaded.com.google.common.net.InternetDomainName;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

// Parse HTML of top category website in Alexa
// View Https://mail.google.com/ Https://docs.google.com/ as one google.com 
// The amount of information is already contained in web traffic
public class AlexaTopCategorySite {

	private static AlexaTopCategorySite instance = new AlexaTopCategorySite();

	private AlexaTopCategorySite() {
	}

	public static AlexaTopCategorySite getInstance() {
		return instance;
	}

	public HashSet<String> getSitesInCategory(String url) throws IOException {

		HashSet<String> siteSet = new HashSet<String>();

		// I'm not a robot
		Document doc = Jsoup
				.connect(url)
				.userAgent(
						"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36")
				.get();
		
		Elements links = doc.select("a[href]");

		// System.out.println(links.size());
		Iterator<Element> iterator = links.iterator();
		while (iterator.hasNext()) {
			String site = iterator.next().text();
			// Check if the element is a top website
			if (site.contains(".")) {
				// System.out.println(site);
				// Case1: Games.yahoo.com & not a public suffix : gov.ph
				if (!site.contains("/") && !site.contains("Http")
						&& !site.contains("www")) {
					// If not topDomain using top domain
					// Games.yahoo.com-> yahoo.com
					String domain = "www." + site;
					String topDomain = InternetDomainName.from(domain)
							.topPrivateDomain().toString();
					siteSet.add(topDomain);
				}
				// Case2: En.wikipedia.org/wiki/Main_Page
				else if (site.contains("/") && !site.contains("Http")) {
					String domain = site.substring(0, site.indexOf("/"));
					String topDomain = InternetDomainName.from(domain)
							.topPrivateDomain().toString();
					siteSet.add(topDomain);
				}
				// Case3: process bad string : Https://www.bet-at-home.com/,
				// Https://www.google.com/adsense
				else if (site.contains("Https://")) {
					String domainWithSlash = site.substring(
							site.indexOf("//") + 2, site.length());
					int slashPos = domainWithSlash.indexOf("/");
					String domain = domainWithSlash.substring(0, slashPos);
					String topDomain = InternetDomainName.from(domain)
							.topPrivateDomain().toString();
					siteSet.add(topDomain);
				}
				// Case4:
				else {
					System.out.println(site);
					break;
				}
			}
		}

		return siteSet;
	}

	public HashMap<Long, String> getRankedSitesInCategory(String url)
			throws IOException {

		return null;
	}

}
