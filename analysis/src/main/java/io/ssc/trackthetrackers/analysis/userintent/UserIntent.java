package io.ssc.trackthetrackers.analysis.userintent;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;

// Get the user intent distribution
public class UserIntent {

	private static String argPathSiteValue = Config.get("analysis.results.path") + "siteValue.csv";
	private static String argPathToPLD = Config.get("webdatacommons.pldfile.unzipped");
	private static String argPathToEmbedArcs = Config.get("analysis.results.path") + "distinctArcCompanyLevel";
	private static String argPathOut = Config.get("analysis.results.path") + "userIntent";

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> embedArcs = ReaderUtils.readArcs(env, argPathToEmbedArcs);

		// Convert the input as (nodeName, value)
		DataSet<Tuple2<String, Double>> nodeAndValue = ReaderUtils.readStringAndValue(env, argPathSiteValue);

		// Read PLD index
		DataSet<Tuple2<String, Long>> pldNodes = ReaderUtils.readNameAndId(env, argPathToPLD);

		// Get first party ID and value (ID, value)
		DataSet<Tuple2<Long, Double>> NodeIdAndValue = nodeAndValue.join(pldNodes).where(0).equalTo(0).projectSecond(1).projectFirst(1);

		// Get (Company, 1st party, value of 1st party)
		DataSet<Tuple2<Long, Double>> arcsValues = embedArcs.join(NodeIdAndValue).where(1).equalTo(0).projectFirst(0).projectSecond(1);

		DataSet<Tuple2<Long, Double>> companyIDAndValue = arcsValues.groupBy(0).aggregate(Aggregations.SUM, 1);

		companyIDAndValue.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

	}
}
