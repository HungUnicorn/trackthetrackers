package io.ssc.trackthetrackers.analysis.extraction.company;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.CompanyParser;
import io.ssc.trackthetrackers.analysis.DomainParser;

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
	private static String argPathToDomainCompany = "/home/sendoh/trackthetrackers/analysis/src/resources/company/domainCompanyMapping";

	private static String argPathOut = Config.get("analysis.results.path") + "companyIndex.tsv";

	public static void main(String[] args) throws Exception {
		Set<String> companySet = new HashSet<String>();
		companySet = readCompany(argPathToDomainCompany);
		writeCompanyWithSurrogatekey(argPathOut, companySet);

	}

	public static Set<String> readCompany(String filePath) throws IOException {

		HashSet<String> companySet = new HashSet<String>();
		FileReader fileReader = new FileReader(argPathToDomainCompany);
		String line;

		BufferedReader bufferedReader = new BufferedReader(fileReader);

		while ((line = bufferedReader.readLine()) != null) {
			String company = line.substring(line.indexOf(",") + 1).trim();
			String companyRemoveComma = CompanyParser.readCompanyCommaRemoved(company);
			if (CompanyParser.isCompanyStrict(companyRemoveComma)) {
				companySet.add(companyRemoveComma);
			}

		}
		bufferedReader.close();
		System.out.println("read");
		System.out.println("#company: " + companySet.size());
		return companySet;
	}

	public static void writeCompanyWithSurrogatekey(String filePath, Set<String> companySet) throws IOException {
		FileWriter fileWriter = new FileWriter(filePath);

		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		int key = 0;
		bufferedWriter.write("NotInWHOis" + "\t" + key);
		bufferedWriter.newLine();
		key++;

		Iterator<String> iterator = companySet.iterator();
		while (iterator.hasNext()) {
			String company = iterator.next();
			bufferedWriter.write(company + "\t" + key);
			bufferedWriter.newLine();
			key++;
		}
		bufferedWriter.close();
		System.out.println("write");

	}
}
