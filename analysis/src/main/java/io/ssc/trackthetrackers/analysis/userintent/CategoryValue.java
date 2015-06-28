package io.ssc.trackthetrackers.analysis.userintent;

import java.io.IOException;
import java.util.Map;

/* Give a csv file that showing the mapping between category and value
 * Process google adwords output 
 */
public class CategoryValue {

	private static String argPathWordValue = "/home/sendoh/datasets/UserIntent/wordValue.csv";
	private static String argPathCategoryValue = "/home/sendoh/datasets/UserIntent/categoryValue.csv";

	public static void main(String args[]) throws IOException {
		FileIO fileIO = new FileIO();
		Map<String, Float> categoryValueMap;
		categoryValueMap = fileIO.readCategoryValue(argPathWordValue);
		fileIO.writeFile(argPathCategoryValue, categoryValueMap);
	}
}
