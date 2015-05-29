package io.ssc.trackthetrackers.analysis.etl;

import io.ssc.trackthetrackers.Config;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

// Give surrogate key (String, int)
public class ThirdPartyIndexGenerator {
	private static String argPathToDistinctThirdParty = Config.get("analysis.results.path") + "distinctThirdParty.csv";

	private static String argPathOut = Config.get("analysis.results.path") + "thirdPartyIndex";

	public static void main(String[] args) throws Exception {
		giveSurrogateKey(argPathToDistinctThirdParty, argPathOut);

	}

	public static void giveSurrogateKey(String inputFilePath, String outputFilePath) throws IOException {

		System.out.println("Start");
		FileReader fileReader = new FileReader(inputFilePath);
		BufferedReader bufferedReader = new BufferedReader(fileReader);

		FileWriter fileWriter = new FileWriter(outputFilePath);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		int key = 0;
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			bufferedWriter.write(line + "," + key);
			key++; 
			bufferedWriter.newLine();
		}
		// number of third parties
		System.out.println(key);
		bufferedReader.close();
		bufferedWriter.close();
		System.out.println("End");

	}

}
