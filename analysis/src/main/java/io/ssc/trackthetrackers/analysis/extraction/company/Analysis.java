package io.ssc.trackthetrackers.analysis.extraction.company;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.TreeMap;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.extraction.DomainParser;

public class Analysis {

	private static String domainCompanyPath = "/home/sendoh/trackthetrackers/analysis/src/resources/company/domainCompanyMapping";
	private static String domainLookupPath = Config.get("analysis.results.path") + "topTrafficThirdParty";
	private static String whoisAnalysisPath = Config.get("analysis.results.path") + "whoisAnalysis.csv";

	private static int interval = 100;

	public static void main(String args[]) throws IOException {
		
		//crawlProcess();
		companyRepeatAmount();
	}

	public static void companyRepeatAmount() throws IOException {
		FileIO fileIO = new FileIO(domainCompanyPath, domainLookupPath);
		DomainParser domainParser = new DomainParser();

		Set<String> lookupSortedSet = new HashSet<String>();
		Map<String, String> domainCompanyMap = new HashMap<String, String>();
		lookupSortedSet = fileIO.readAsSortedSet();
		domainCompanyMap = fileIO.readCleanCompany();

		int repeat = 0;
		int order = 0;
		Set<String> knownCompanySet = new HashSet<String>();
		Map<Integer, Integer> SameCompanyCountInterval = new TreeMap<Integer, Integer>();

		// Show how many repeat companies in the interval
		for (String domain : lookupSortedSet) {
			String company = domainCompanyMap.get(domain);

			if (knownCompanySet.contains(company) && domainParser.isCompany(company)) {
				// System.out.println(domain + "," + company);
				repeat++;
			} else {
				knownCompanySet.add(company);
			}

			order++;

			if (order % interval == 0) {
				SameCompanyCountInterval.put(order, repeat);

			}
			if (order > 29000) {
				break;
			}
		}
		fileIO.writeWhoisAnalysis(SameCompanyCountInterval, whoisAnalysisPath);
	}

	// Amount of crawl at each interval
	public static void crawlAmount() throws IOException {
		FileIO fileIO = new FileIO(domainCompanyPath, domainLookupPath);
		Set<String> lookupSortedSet = new HashSet<String>();
		Map<String, String> domainCompanyMap = new HashMap<String, String>();
		Map<Integer, Integer> intervalMap = new TreeMap<Integer, Integer>();

		lookupSortedSet = fileIO.readAsSortedSet();
		domainCompanyMap = fileIO.readDomainKnown();
		DomainParser domainParser = new DomainParser();

		int maxOrder = crawlProcess();

		int numMeaningfulResult = 0;
		int order = 0;
		System.out.println("--------------");
		System.out.println("Top, MeaningfulResult");

		// (Interval, number of meaningful result)
		for (String entry : lookupSortedSet) {

			// Ignore the domain name for example, .com, .gov
			Pattern SEPARATOR = Pattern.compile("[.]");
			String tokens[] = SEPARATOR.split(entry);

			if (tokens.length < 2) {
				break;
			}

			String tld = tokens[tokens.length - 1];
			String potentialTld = tokens[tokens.length - 2];

			// TLD
			if (tld.equalsIgnoreCase("gov") || tld.equalsIgnoreCase("edu") || tld.equalsIgnoreCase("mil")) {
				// ccTLD e.g. aa.gov.tw
				if (potentialTld.equalsIgnoreCase("gov") || potentialTld.equalsIgnoreCase("edu") || potentialTld.equalsIgnoreCase("mil")) {
					break;

				}
			}

			String company = domainCompanyMap.get(entry);
			if (company != null) {
				if (domainParser.isCompany(company)) {
					numMeaningfulResult++;
				}
			}

			// See the top 100
			if ((company == null || !domainParser.isCompany(company)) && order < 101) {
				System.out.println(entry);
			}

			order++;
			if (order % interval == 0) {
				intervalMap.put(order, numMeaningfulResult);
				// System.out.println(order + "," + numMeaningfulResult);
			}

			// Crawl progress
			if (order > 40000) {
				break;
			}

		}
		incresingNumberInInterval(intervalMap);
	}

	// (Interval, increasing number of meaningful result)
	// increasing number of meaningful result = number of meaningful result
	// in top 200 - number of meaningful result in top 100
	private static void incresingNumberInInterval(Map<Integer, Integer> intervalMap) throws IOException {
		FileIO fileIO = new FileIO();
		Map<Integer, Integer> incresingNumberInInterval = new TreeMap<Integer, Integer>();
		Iterator<Entry<Integer, Integer>> iterator = intervalMap.entrySet().iterator();
		Integer previousInterval = new Integer(interval);
		while (iterator.hasNext()) {
			Entry<Integer, Integer> current = iterator.next();

			Integer intervalOrder = current.getKey();
			Integer value = current.getValue();

			if (previousInterval.equals(intervalOrder)) {
				incresingNumberInInterval.put(intervalOrder, value - 0);
			}

			else {
				Integer delta = value - intervalMap.get(previousInterval);

				incresingNumberInInterval.put(intervalOrder, delta);

				previousInterval = current.getKey();
			}
		}

		fileIO.writeWhoisAnalysis(incresingNumberInInterval, whoisAnalysisPath);
	}

	// The current crawl progress. Put the last entry in domain company mapping file
	private static int crawlProcess() throws IOException {
		FileIO fileIO = new FileIO(domainCompanyPath, domainLookupPath);
		Set<String> lookupSortedSet = new HashSet<String>();
		lookupSortedSet = fileIO.readAsSortedSet();
		int maxOrder = 0;
		
		for (String entry : lookupSortedSet) {
			maxOrder++;
			if (entry.equalsIgnoreCase("osoyou.com")) {
				break;
			}
		}
		System.out.println("current crawl progress: " + maxOrder);
		return maxOrder;
	}
}
