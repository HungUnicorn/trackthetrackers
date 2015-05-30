/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Hung Chang
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.ssc.trackthetrackers.analysis.extraction.company;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.extraction.DomainParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.flink.shaded.com.google.common.net.InternetDomainName;

// Extract domains' companies incrementally every X seconds via WHOISComapnyLookup.java
// Input format:
// google.com \n facebook.com \n yahoo.com \n
// Output: google.com, Google Inc. \n facebook.com, Facebook, Inc. \n
//
// If encountering "Connection reset", it's due to killing the current connection because it takes too long 
// mainly due to connection refused or irregular whois response.
// If encountering "Connection refused", this process needs to wait for days.
// If encountering "No such element", that whois response doesn't contain administrative organization information.
// Can refer to the site : https://who.is/
/**
 * ICANN term of use: All results shown are captured from registries and/or
 * registrars and are framed in real-time. ICANN does not generate, collect,
 * retain or store the results shown other than for the transitory duration
 * necessary to show these results in response to real-time queries.* These
 * results are shown for the sole purpose of assisting you in obtaining
 * information about domain name registration records and for no other purpose.
 * You agree to use this data only for lawful purposes and further agree not to
 * use this data (i) to allow, enable, or otherwise support the transmission by
 * email, telephone, or facsimile of mass unsolicited, commercial advertising,
 * or (ii) to enable high volume, automated, electronic processes to collect or
 * compile this data for any purpose, including without limitation mining this
 * data for your own personal or commercial purposes. ICANN reserves the right
 * to restrict or terminate your access to the data if you fail to abide by
 * these terms of use. ICANN reserves the right to modify these terms at any
 * time. By submitting a query, you agree to abide by these terms. See
 * http://whois.icann.org/
 */
public class CrawlWHOIS {

	// Every x MILLISECONDS get WHOIS once
	private static int WHOISdelay = 100;

	// If lookup takes too long, it's mainly due to connection refused or
	// irregular whois response, so kill this lookup and continue to the next
	// lookup
	private static int WHOISTimeout = 2;

	private static String domainCompanyPath = "/home/sendoh/trackthetrackers/analysis/src/resources/company/DomainAndCompany.csv";
	private static String domainLookupPath = Config.get("analysis.results.path") + "topTrafficThirdParty";
	private static String exceptionDomainPath = "/home/sendoh/trackthetrackers/analysis/src/resources/company/ExceptionDomain.csv";
	private static FileIO fileIO;

	private static boolean INCREMENTAL_EXTRACTION;

	private static HashMap<String, String> domainCompanyMap;
	private static HashMap<String, String> domainKnownMap;
	private static Set<String> domainCheckingSet;
	private static Set<String> exceptionDomainSet;

	private static String company = "N/A";

	private static DomainParser domainParser;

	public static void main(String args[]) throws Exception {
		
		crawl();
		//test("ajax.googleapis");
	}

	public static void test(String domain) throws Exception {
		domainParser = new DomainParser();
		String processedDomain = domainParser.whoisDomain("gov.ph");
		System.out.println("test: " + processedDomain);
		WhoisParser whoisParser = new WhoisParser();
		whoisParser.getFullResult(processedDomain);
		System.out.println("tested");

	}

	public static void crawl() throws IOException, InterruptedException {
		domainParser = new DomainParser();
		fileIO = new FileIO(domainCompanyPath, domainLookupPath, exceptionDomainPath);
		// Incremental check: Check if the domain already processed before
		INCREMENTAL_EXTRACTION = fileIO.checkProcessBefore();

		domainKnownMap = new HashMap<String, String>();
		// Prepare the domains want to check
		domainCheckingSet = new HashSet<String>();
		exceptionDomainSet = new HashSet<String>();
		domainCheckingSet = fileIO.readAsSortedSet();

		if (INCREMENTAL_EXTRACTION) {
			domainKnownMap = fileIO.readDomainKnown();
			exceptionDomainSet = fileIO.readAsSet();
			domainCheckingSet.removeAll(domainKnownMap.keySet());
			domainCheckingSet.removeAll(exceptionDomainSet);
		}

		System.out.println("domains lookup: " + domainCheckingSet.size() + " takes " + (int) domainCheckingSet.size() * 2 * WHOISdelay / 6000
				+ " minutes maximum");

		domainCompanyMap = new HashMap<String, String>();

		for (String domain : domainCheckingSet) {
			// Whois query needs top domain
			// If not topDomain, using top domain get WHOIS data
			String topDomain = domainParser.whoisDomain(domain);

			String tld = domainParser.getTLD(topDomain);

			// Possibly it's already known
			if (!topDomain.equalsIgnoreCase(domain)) {
				if (domainCompanyMap.containsKey(topDomain)) {
					company = domainCompanyMap.get(topDomain);
					domainCompanyMap.put(domain, company);
				} else if (domainKnownMap.containsKey(topDomain)) {
					company = domainKnownMap.get(topDomain);
					domainCompanyMap.put(domain, company);
				}
			}

			// If it's a ccTLD, check if it contains symbol (ex.
			// amazon.jp, google.fr), the length is 2
			else if (tld.length() < 3) {
				String checkingSymbol = domainParser.getSymbol(domain);

				Iterator<Entry<String, String>> iterator = domainKnownMap.entrySet().iterator();

				while (iterator.hasNext()) {
					Entry<String, String> currentEntry = iterator.next();
					String domainKnown = currentEntry.getKey();
					String companyKnown = currentEntry.getValue();

					String knownSymbols = domainParser.getSymbol(domainKnown);

					if (knownSymbols.equalsIgnoreCase(checkingSymbol))
						domainCompanyMap.put(domain, companyKnown);
				}
			}

			// Start WHOIS
			else {
				WhoisParser whoisParser = new WhoisParser();
				exceptionDomainSet = new HashSet<String>();

				// If lookup takes too long, it's mainly due to connection
				// refused or irregular whois response,
				// so kill this lookup and continue to the next lookup
				final ExecutorService executorService = Executors.newSingleThreadExecutor();
				Future<?> future = null;
				try {
					future = executorService.submit(() -> {
						try {
							company = whoisParser.getCompany(topDomain);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					});
					future.get(WHOISTimeout, TimeUnit.SECONDS);
				} catch (Exception e) {
					if (future != null) {
						future.cancel(true);
					}
				}

				if (!company.equalsIgnoreCase("N/A"))
					domainCompanyMap.put(domain, company);

				// Write the result if cannot get the company
				else {
					exceptionDomainSet.add(domain);
					fileIO.writeCheckedDomainToDisk(domainCompanyMap);
					FileIO.writeExceptionDomainToDisk(exceptionDomainSet);
					System.out.println("Writting...");

					domainCompanyMap.clear();
					exceptionDomainSet.clear();

				}
				// Wait X MILLISECONDS
				TimeUnit.MILLISECONDS.sleep(WHOISdelay);
			}
		}

		fileIO.writeCheckedDomainToDisk(domainCompanyMap);
		System.out.println("End WHOIS Extraction");
	}
}
