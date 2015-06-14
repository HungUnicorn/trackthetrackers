package io.ssc.trackthetrackers.analysis.userintent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class FileIO {

	public Map<String, Map<String, String>> readSiteCategory(String filePath) throws IOException {

		File folder = new File(filePath);
		Map<String, Map<String, String>> siteCategoryMap = new HashMap<String, Map<String, String>>();

		Pattern SEPERATOR = Pattern.compile("[,]");

		for (final File fileEntry : folder.listFiles()) {
			FileReader fileReader = new FileReader(fileEntry);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			String category = null;
			String site = null;
			String line = null;
			Map<String, String> tempMap = new HashMap<String, String>();

			while ((line = bufferedReader.readLine()) != null) {
				String tokens[] = SEPERATOR.split(line);
				site = tokens[0];
				category = tokens[1].toLowerCase();
				tempMap.put(site, category);
			}

			bufferedReader.close();

			siteCategoryMap.put(category, tempMap);
			//System.out.println("Read " + fileEntry.getName());
		}
		System.out.println("Read site category");
		return siteCategoryMap;

	}

	public Map<String, Float> readCategoryValue(String filePath) throws IOException {

		String[] FILE_HEADER_MAPPING = { "Ad group", "Keyword", "Currency", "Avg. Monthly Searches (exact match only)", "Competition",
				"Suggested bid", "Impr. share", "In account?", "In plan?", "Extracted From" };

		String AD_GROUP = "Ad group";
		String KEYWORD = "Keyword";
		String SUGGESTED_BID = "Suggested bid";

		FileReader fileReader = null;
		CSVParser csvFileParser = null;

		Map<String, Float> categoryValue = new HashMap<String, Float>();

		// Create the CSVFormat object with the header mapping
		CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING);

		try {
			fileReader = new FileReader(filePath);
			csvFileParser = new CSVParser(fileReader, csvFileFormat);

			// Get a list of CSV file records
			List csvRecords = csvFileParser.getRecords();

			// Read the CSV file records starting from the second record to skip
			// the header
			for (int i = 1; i < csvRecords.size(); i++) {
				CSVRecord record = (CSVRecord) csvRecords.get(i);
				String adGroup = record.get(AD_GROUP);
				if (adGroup.equalsIgnoreCase("Seed Keywords")) {
					String category = record.get(KEYWORD).toLowerCase();
					categoryValue.put(category, Float.parseFloat(record.get(SUGGESTED_BID)));
				}
			}
		}

		catch (Exception e) {
			System.out.println("Error in CsvFileReader");
			e.printStackTrace();

		} finally {
			try {
				fileReader.close();
				csvFileParser.close();
			} catch (IOException e) {
				System.out.println("Error while closing fileReader/csvFileParser");
				e.printStackTrace();
			}

		}
		System.out.println("Read keyword value");
		return categoryValue;
	}

	public static void writeFile(String filePath, Map<String, Float> siteValueMap) throws IOException {
		FileWriter fileWriter = new FileWriter(filePath);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		Iterator<Entry<String, Float>> iterator = siteValueMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, Float> entry = iterator.next();
			bufferedWriter.write(entry.getKey() + "," + entry.getValue());
			bufferedWriter.newLine();
		}
		bufferedWriter.close();
		System.out.println("Write");
	}
}
