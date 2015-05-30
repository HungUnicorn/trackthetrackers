package io.ssc.trackthetrackers.analysis.extraction.category;

import io.ssc.trackthetrackers.Config;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class CombileResult {

	private static String argPathToSiteCategory = Config
			.get("analysis.results.path") + "SiteCategory" + "/";

	private static String argPathOut = Config.get("analysis.results.path")
			+ "allCategorySites";

	public static void main(String[] args) throws Exception {
		ArrayList<String> siteList = new ArrayList<String>();
		siteList = readCategoryFile(argPathToSiteCategory);
		writeFile(argPathOut, siteList);

	}

	public static ArrayList<String> readCategoryFile(String filePath)
			throws IOException {

		File folder = new File(filePath);
		ArrayList<String> siteList = new ArrayList<String>();

		for (final File fileEntry : folder.listFiles()) {

			FileReader fileReader = new FileReader(fileEntry);
			String line;
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {
				siteList.add(line);
			}

			bufferedReader.close();

			System.out.println("Read " + fileEntry.getName());

		}
		return siteList;

	}

	public static void writeFile(String filePath,
			ArrayList<String> siteList) throws IOException {
		FileWriter fileWriter = new FileWriter(filePath);

		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		Iterator<String> iterator = siteList.iterator();
		while (iterator.hasNext()) {
			String line = iterator.next();
			bufferedWriter.write(line);
			bufferedWriter.newLine();
		}
		bufferedWriter.close();
		System.out.println("write");

	}

}
