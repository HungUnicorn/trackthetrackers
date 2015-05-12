package io.ssc.trackthetrackers.analysis.publisher.category;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.flink.shaded.com.google.common.net.InternetDomainName;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

// Crawl webiste's category in Alexa
// View Https://mail.google.com/ Https://docs.google.com/ as google.com 
// Singleton design pattern
public class AlexaTopCategorySite {

	private static AlexaTopCategorySite instance = new AlexaTopCategorySite();

	private AlexaTopCategorySite() {
	}

	public static AlexaTopCategorySite getInstance() {
		return instance;
	}

	public HashSet<String> getSitesInCategory(String url) throws IOException {

		HashSet<String> siteSet = new HashSet<String>();

		Document doc = Jsoup.connect(url).get();
		Elements links = doc.select("a[href]");

		// System.out.println(links.size());
		Iterator<Element> iterator = links.iterator();
		while (iterator.hasNext()) {
			String site = iterator.next().text();
			// Check if the element is a website
			if (site.contains(".")) {
				// ignore e.g.
				// Https://productforums.google.com/forum/#!forum/blogger
				String splitArray[] = site.split("/");
				if (splitArray.length > 3) {
					break;
				}
				String tld = site.substring(site.lastIndexOf(".") + 1,
						site.length());
				// Not under a public suffix
				if (tld.equalsIgnoreCase("uk")) {
					break;
				}

				if (!site.contains("/") && !site.contains("Http")) {
					// If not topDomain using top domain
					// Games.yahoo.com-> yahoo.com
					String topDomain = InternetDomainName.from(site)
							.topPrivateDomain().toString();
					siteSet.add(topDomain);
				}
				// process bad string : Https://www.bet-at-home.com/
				else if (site.contains("Https://www")) {
					String domain = site.substring(site.indexOf(".") + 1,
							site.lastIndexOf("/"));
					// handle Https://www.google.com/adsense
					if (domain.contains("/")) {
						domain = domain.substring(0, domain.lastIndexOf("/"));
					}
					String topDomain = InternetDomainName.from(domain)
							.topPrivateDomain().toString();
					siteSet.add(topDomain);
				}

				// process Https://twitter.com/
				else if (!site.contains("www") && site.contains("Https")) {
					String domain = site.substring(site.indexOf("//") + 2,
							site.lastIndexOf("/"));

					// handle Https://www.google.com/adsense
					if (domain.contains("/")) {
						domain = domain.substring(0, site.lastIndexOf("/"));
					}

					String topDomain = InternetDomainName.from(domain)
							.topPrivateDomain().toString();
					siteSet.add(topDomain);
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
