package io.ssc.trackthetrackers.analysis.extraction.company;

import io.ssc.trackthetrackers.analysis.extraction.DomainParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

public class FileIO {

	private String domainCompanyPath, domainLookupParth;
	private static String exceptionDomationPath;

	public FileIO() {
	}

	public FileIO(String domainComapanyPath, String domainLookupPath) {
		this.domainCompanyPath = domainComapanyPath;
		this.domainLookupParth = domainLookupPath;
	}

	public FileIO(String domainComapanyPath, String domainLookupPath, String exceptionDomainPath) {
		this.domainCompanyPath = domainComapanyPath;
		this.domainLookupParth = domainLookupPath;
		this.exceptionDomationPath = exceptionDomainPath;
	}

	// Check if the output file already exist
	public boolean checkProcessBefore() {
		File domainknownFile = new File(domainCompanyPath);
		if (domainknownFile.exists())
			return true;
		else
			return false;
	}

	// Write the result to disk
	public static void writeExceptionDomainToDisk(Set<String> excetptionDomainSet) throws IOException {

		FileWriter fileWriter = new FileWriter(exceptionDomationPath, true);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		Iterator<String> iterator = excetptionDomainSet.iterator();

		while (iterator.hasNext()) {
			String entry = iterator.next();
			bufferedWriter.write(entry + "\n");
		}

		bufferedWriter.close();
		fileWriter.close();
	}

	// Prepare the domains already checked
	public HashMap<String, String> readDomainKnown() throws IOException {

		HashMap<String, String> domainKnownMap = new HashMap<String, String>();
		FileReader fileReader = new FileReader(domainCompanyPath);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = bufferedReader.readLine();

		while (line != null) {
			String[] domainAndCompany = line.split(",");
			String domainKnown = domainAndCompany[0];
			String company = domainAndCompany[1];
			if (!company.equalsIgnoreCase("null"))
				domainKnownMap.put(domainKnown, company);
			line = bufferedReader.readLine();
		}

		bufferedReader.close();
		fileReader.close();

		return domainKnownMap;
	}

	// Prepare the domains already checked
	public HashMap<String, String> readCleanCompany() throws IOException {
		DomainParser domainParser = new DomainParser();
		HashMap<String, String> domainKnownMap = new HashMap<String, String>();
		FileReader fileReader = new FileReader(domainCompanyPath);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = bufferedReader.readLine();

		while (line != null) {
			String[] domainAndCompany = line.split(",");
			String domainKnown = domainAndCompany[0];
			String company = domainAndCompany[1];
			if (!company.isEmpty()) {
				if (domainParser.isCompany(company)) {
					domainKnownMap.put(domainKnown, company);
				}
			}
			line = bufferedReader.readLine();
		}

		bufferedReader.close();
		fileReader.close();

		return domainKnownMap;
	}

	// Read domain into a map descending sorted on the value and return a sorted
	// set
	public Set<String> readAsSortedSet() throws IOException {

		FileReader fileReader = new FileReader(domainLookupParth);
		BufferedReader bufferedReader = new BufferedReader(fileReader);

		HashMap<String, Double> map = new HashMap<String, Double>();

		Pattern SEPARATOR = Pattern.compile("[ \t,]");

		String line = bufferedReader.readLine();
		while (line != null) {

			String[] tokens = SEPARATOR.split(line);
			String name = tokens[0];
			Double value = Double.parseDouble(tokens[1]);
			map.put(name, value);
			line = bufferedReader.readLine();
		}
		bufferedReader.close();
		fileReader.close();

		map = (HashMap<String, Double>) sortByValueDesc(map);
		Set<String> sortedset = map.keySet();
		System.out.println("Size of reading set " + sortedset.size());
		return sortedset;
	}

	public Set<String> readAsSet() throws IOException {

		FileReader fileReader = new FileReader(exceptionDomationPath);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		Set<String> set = new HashSet<String>();

		String line = bufferedReader.readLine();
		while (line != null) {

			set.add(line);
			line = bufferedReader.readLine();
		}
		bufferedReader.close();
		fileReader.close();
		return set;
	}

	public Set<String> readIndex() throws IOException {

		FileReader fileReader = new FileReader(domainLookupParth);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		Set<String> set = new HashSet<String>();

		Pattern SEPARATOR = Pattern.compile("[ \t,]");
		String line = bufferedReader.readLine();

		while (line != null) {
			String tokens[] = SEPARATOR.split(line);
			set.add(tokens[0]);
			line = bufferedReader.readLine();
		}
		bufferedReader.close();
		fileReader.close();
		System.out.println("Size of reading set " + set.size());
		return set;
	}

	// Write the result to disk
	public void writeCheckedDomainToDisk(HashMap<String, String> domainCompanyMap) throws IOException {

		FileWriter fileWriter = new FileWriter(domainCompanyPath, true);

		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		Iterator<Entry<String, String>> iterator = domainCompanyMap.entrySet().iterator();

		while (iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			bufferedWriter.write(entry.getKey() + "," + entry.getValue() + "\n");
		}

		bufferedWriter.close();
		fileWriter.close();
	}

	// Write the result to disk
	public void writeWhoisAnalysis(Map<Integer, Integer> map, String filePath) throws IOException {

		FileWriter fileWriter = new FileWriter(filePath);

		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		Iterator<Entry<Integer, Integer>> iterator = map.entrySet().iterator();

		while (iterator.hasNext()) {
			Entry<Integer, Integer> entry = iterator.next();
			bufferedWriter.write(entry.getKey() + "," + entry.getValue() + "\n");
		}

		bufferedWriter.close();
		fileWriter.close();
	}

	private <K, V extends Comparable<? super V>> Map<K, V> sortByValueDesc(Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			@Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return (o2.getValue().compareTo(o1.getValue()));
			}
		});

		Map<K, V> result = new LinkedHashMap<>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}
}
