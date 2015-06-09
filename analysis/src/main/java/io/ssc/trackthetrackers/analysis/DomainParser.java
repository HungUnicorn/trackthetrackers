package io.ssc.trackthetrackers.analysis;

import java.util.regex.Pattern;

import org.apache.flink.shaded.com.google.common.net.InternetDomainName;

public class DomainParser {

	public static String alexaDomain(String site) {
		String topDomain = null;

		if (!site.contains("/") && !site.contains("Http") && !site.contains("www")) {
			// If not topDomain using top domain
			// Games.yahoo.com-> yahoo.com
			// for gov.ph need to be www.gov.ph
			String domain = "www." + site;
			topDomain = InternetDomainName.from(domain).topPrivateDomain().toString();

		}
		// Case2: En.wikipedia.org/wiki/Main_Page
		else if (site.contains("/") && !site.contains("Http")) {
			String domain = site.substring(0, site.indexOf("/"));
			topDomain = InternetDomainName.from(domain).topPrivateDomain().toString();

		}
		// Case3: process bad string : Https://www.bet-at-home.com/,
		// Https://www.google.com/adsense
		else if (site.contains("Https://")) {
			String domainWithSlash = site.substring(site.indexOf("//") + 2, site.length());
			int slashPos = domainWithSlash.indexOf("/");
			String domain = domainWithSlash.substring(0, slashPos);
			topDomain = InternetDomainName.from(domain).topPrivateDomain().toString();

		}
		return topDomain;
	}

	public static String whoisDomain(String site) {
		String topDomain = null;

		final Pattern SEPARATOR = Pattern.compile("[.]");

		String tokens[] = SEPARATOR.split(site);

		// Ignore aaaa.co.jp and and handle font.googleapis.com,
		// Games.yahoo.com->
		// yahoo.com
		if (!site.contains("www") && tokens.length > 2 && tokens[1].length() > 2) {
			// skip token[0]
			topDomain = tokens[1] + "." + tokens[2];

		} else {
			topDomain = "www." + site;
			topDomain = InternetDomainName.from(topDomain).topPrivateDomain().toString();
		}

		return topDomain;
	}

	public static String getTLD(String domain) {

		return domain.substring(domain.lastIndexOf(".") + 1).trim().toLowerCase();
	}

	// If it's a ccTLD, check if it contains symbol (ex.
	// amazon.jp, google.fr), the length is 2
	public static String getSymbol(String domain) {

		return domain.split("\\.")[0];
	}

	// Refer to http://en.wikipedia.org/wiki/List_of_Internet_top-level_domains
	public static boolean isCCTLD(String tld) {
		if (!tld.equalsIgnoreCase("gov") && !tld.equalsIgnoreCase("net") && !tld.equalsIgnoreCase("com") && !tld.equalsIgnoreCase("org")
				&& !tld.equalsIgnoreCase("edu") && !tld.equalsIgnoreCase("mil")) {
			return true;
		} else {
			return false;
		}

	}

	public static boolean isBusinessDomain(String tld) {
		return tld.equalsIgnoreCase("net") || tld.equalsIgnoreCase("com") || tld.equalsIgnoreCase("org");
	}
}
