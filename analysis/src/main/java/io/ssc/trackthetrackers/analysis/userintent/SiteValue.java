package io.ssc.trackthetrackers.analysis.userintent;

import io.ssc.trackthetrackers.Config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class SiteValue {

	private static String argPathWordValue = "/home/sendoh/datasets/wordValue.csv";
	private static String argPathToSiteCategory = "/home/sendoh/datasets/SiteCategory/";

	private static String argPathOut = Config.get("analysis.results.path") + "siteValue.csv";

	public static void main(String args[]) throws IOException {
		FileIO fileIO = new FileIO();

		Map<String, Float> categoryValueMap;
		categoryValueMap = fileIO.readCategoryValue(argPathWordValue);

		Map<String, Float> siteValueMap = new HashMap<String, Float>();

		Map<String, Map<String, String>> siteCategoryMap;
		siteCategoryMap = fileIO.readSiteCategory(argPathToSiteCategory);

		for (Entry<String, Map<String, String>> siteCategory : siteCategoryMap.entrySet()) {

			String category = siteCategory.getKey();
			// System.out.println(category);
			float thisCategoryValue = categoryValueMap.get(category);
			Map<String, String> thisCategorySite = siteCategory.getValue();
			
			for (Entry<String, String> entry : thisCategorySite.entrySet()) {
				String site = entry.getKey();

				// Site exists in different category
				if (siteValueMap.get(site) != null) {
					float currentValue = siteValueMap.get(site);
					siteValueMap.put(entry.getKey(), currentValue + thisCategoryValue);
				}

				else {
					siteValueMap.put(entry.getKey(), thisCategoryValue);
				}
			}
		}
		fileIO.writeFile(argPathOut, siteValueMap);
	}
}
