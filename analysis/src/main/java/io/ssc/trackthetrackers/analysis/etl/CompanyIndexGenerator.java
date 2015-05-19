package io.ssc.trackthetrackers.analysis.etl;

import io.ssc.trackthetrackers.Config;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

// Give surrogate key for company
public class CompanyIndexGenerator {
	private static String argPathToDomainCompany = "/home/sendoh/trackthetrackers/analysis/src/resources/company/DomainAndCompany.csv";

	private static String argPathOut = Config.get("analysis.results.path")
			+ "companyWithIndex";

	public static void main(String[] args) throws Exception {
		Set<String> companySet = new HashSet<String>();
		companySet = readCompany(argPathToDomainCompany);
		writeCompanyWithSurrogatekey(argPathOut, companySet);

	}

	public static Set<String> readCompany(String filePath) throws IOException {

		HashSet<String> companySet = new HashSet<String>();
		FileReader fileReader = new FileReader(argPathToDomainCompany);
		String line;
		Pattern SEPARATOR = Pattern.compile("[\t,]");
		BufferedReader bufferedReader = new BufferedReader(fileReader);

		while ((line = bufferedReader.readLine()) != null) {
			String[] tokens = SEPARATOR.split(line);
			String company = tokens[1];
			if (company.length() > 1 && !company.toLowerCase().contains("privacy")
					&& !company.toLowerCase().contains("proxy")) {
				companySet.add(company);
			}

		}
		bufferedReader.close();
		System.out.println("read");
		System.out.println("#company: " + companySet.size());
		return companySet;
	}

	public static void writeCompanyWithSurrogatekey(String filePath,
			Set<String> companySet) throws IOException {
		FileWriter fileWriter = new FileWriter(filePath);

		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		int key = 0;
		bufferedWriter.write("NotInWHOis" + "," + key);
		bufferedWriter.newLine();
		key++;

		Iterator<String> iterator = companySet.iterator();
		while (iterator.hasNext()) {
			String company = iterator.next();
			bufferedWriter.write(company + "," + key);
			bufferedWriter.newLine();
			key++;
		}
		bufferedWriter.close();
		System.out.println("write");

	}
}
