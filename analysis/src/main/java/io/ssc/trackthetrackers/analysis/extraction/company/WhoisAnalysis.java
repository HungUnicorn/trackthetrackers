package io.ssc.trackthetrackers.analysis.extraction.company;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.CompanyParser;
import io.ssc.trackthetrackers.analysis.DomainParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class WhoisAnalysis {

	private static String centrality = "pr";
	private static String domainCompanyPath = "/home/sendoh/trackthetrackers/analysis/src/resources/company/domainCompanyMapping";
	private static String domainLookupPath = Config.get("analysis.results.path") + "/Traffic/topTrafficThirdParty_" + centrality;

	private static String companyRepeatAmountPath = Config.get("analysis.results.path") + "companyRepeatAmount_" + centrality + ".csv";
	private static String companyAmountPath = Config.get("analysis.results.path") + "companyAmount_" + centrality + ".csv";
	private static String companyStrictAmountPath = Config.get("analysis.results.path") + "companyStrictAmount_" + centrality + ".csv";

	private static int interval = 100;

	public static void main(String args[]) throws IOException {

		int maxLimit = crawlProcess("viewmalaysia.com");
		companyRepeatAmount(maxLimit);
		crawlCompanyStrictAmount(maxLimit);
	}

	// Amount of crawl at each interval for Inc. and Coporation
	public static void crawlCompanyStrictAmount(int maxLimit) throws IOException {
		FileIO fileIO = new FileIO(domainCompanyPath, domainLookupPath);
		Set<String> lookupSortedSet = new HashSet<String>();
		Map<String, String> domainCompanyMap = new HashMap<String, String>();
		Map<Integer, Integer> intervalMap = new TreeMap<Integer, Integer>();

		lookupSortedSet = fileIO.readAsSortedByValueDescSet();
		domainCompanyMap = fileIO.readDomainKnown();

		int numMeaningfulResult = 0;
		int order = 0;
		System.out.println("--------------");
		System.out.println("Top, MeaningfulResult");

		// (Interval, number of meaningful result)
		for (String entry : lookupSortedSet) {

			Pattern SEPARATOR = Pattern.compile("[.]");
			String tokens[] = SEPARATOR.split(entry);

			// Ignore the domain name for example, .com, .gov
			if (tokens.length < 2) {
				break;
			}

			String tld = DomainParser.getTLD(entry);
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
				if (CompanyParser.isCompanyStrict(company)) {
					if (order > 47000) {
						System.out.println(company);
					}
					numMeaningfulResult++;
				}
			}

			order++;
			if (order % interval == 0) {
				intervalMap.put(order, numMeaningfulResult);
				// System.out.println(order + "," + numMeaningfulResult);
			}

			// Crawl progress
			if (order > maxLimit) {
				break;
			}

		}
		incresingNumberInInterval(intervalMap, companyStrictAmountPath);
	}

	// How many company exist repeatedly
	public static void companyRepeatAmount(int maxLimit) throws IOException {
		FileIO fileIO = new FileIO(domainCompanyPath, domainLookupPath);

		Set<String> lookupSortedSet = new HashSet<String>();
		Map<String, String> domainCompanyMap = new HashMap<String, String>();
		lookupSortedSet = fileIO.readAsSortedByValueDescSet();
		domainCompanyMap = fileIO.readCleanCompany();

		int repeat = 0;
		int order = 0;
		Set<String> knownCompanySet = new HashSet<String>();
		Map<Integer, Integer> SameCompanyCountInterval = new TreeMap<Integer, Integer>();

		// Show how many repeat companies in the interval
		for (String domain : lookupSortedSet) {
			String company = domainCompanyMap.get(domain);

			if (knownCompanySet.contains(company) && CompanyParser.isCompany(company)) {
				// System.out.println(domain + "," + company);
				repeat++;
			} else {
				knownCompanySet.add(company);
			}

			order++;

			if (order % interval == 0) {
				SameCompanyCountInterval.put(order, repeat);

			}
			if (order > maxLimit) {
				break;
			}
		}
		fileIO.writeWhoisAnalysis(SameCompanyCountInterval, companyRepeatAmountPath);
	}

	// Amount of crawl at each interval
	public static void crawlCompanyAmount(int maxLimit) throws IOException {
		FileIO fileIO = new FileIO(domainCompanyPath, domainLookupPath);
		Set<String> lookupSortedSet = new HashSet<String>();
		Map<String, String> domainCompanyMap = new HashMap<String, String>();
		Map<Integer, Integer> intervalMap = new TreeMap<Integer, Integer>();

		lookupSortedSet = fileIO.readAsSortedByValueDescSet();
		domainCompanyMap = fileIO.readDomainKnown();

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
				if (CompanyParser.isCompany(company)) {
					numMeaningfulResult++;
				}
			}

			order++;
			if (order % interval == 0) {
				intervalMap.put(order, numMeaningfulResult);
				// System.out.println(order + "," + numMeaningfulResult);
			}

			// Crawl progress
			if (order > maxLimit) {
				break;
			}

		}
		incresingNumberInInterval(intervalMap, companyAmountPath);
	}

	// (Interval, increasing number of meaningful result)
	// increasing number of meaningful result = number of meaningful result
	// in top 200 - number of meaningful result in top 100
	private static void incresingNumberInInterval(Map<Integer, Integer> intervalMap, String filePath) throws IOException {
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

		fileIO.writeWhoisAnalysis(incresingNumberInInterval, filePath);
	}

	// The current crawl progress. Put the last entry in domain company mapping
	// file
	private static int crawlProcess(String lookupDomain) throws IOException {
		FileIO fileIO = new FileIO(domainCompanyPath, domainLookupPath);
		Set<String> lookupSortedSet = new HashSet<String>();
		lookupSortedSet = fileIO.readAsSortedByValueDescSet();
		int maxOrder = 0;

		for (String entry : lookupSortedSet) {
			maxOrder++;
			if (entry.equalsIgnoreCase(lookupDomain)) {
				break;
			}
		}
		System.out.println("current crawl progress: " + maxOrder);
		return maxOrder;
	}
}
