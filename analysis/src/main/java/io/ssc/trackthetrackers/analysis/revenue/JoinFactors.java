package io.ssc.trackthetrackers.analysis.revenue;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Revenue estimation
// Refer to "Follow the money, Understanding Economics of Online Aggregation and Advertising"
public class JoinFactors {

	private static String argPathUserIntent = "/home/sendoh/datasets/UserIntent/userIntent";
	private static String argPathTaffic = "/home/sendoh/datasets/Traffic/trafficCompany_pr";
	private static String argPathRON = "/home/sendoh/datasets/RON/weightedPageRank";
	private static String argPathCompanyIndex = "/home/sendoh/datasets/companyIndex.tsv";

	private static String argPathFactors = Config.get("analysis.results.path") + "/Revenue/" + "joinFactors.csv";

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Double>> userIntent = ReaderUtils.readLongAndValue(env, argPathUserIntent);
		DataSet<Tuple2<Long, Double>> traffic = ReaderUtils.readLongAndValue(env, argPathTaffic);
		DataSet<Tuple2<Long, Double>> ron = ReaderUtils.readLongAndValue(env, argPathRON);

		DataSet<Tuple2<String, Long>> companyIndex = ReaderUtils.readNameWithCommaAndId(env, argPathCompanyIndex);

		// W/o knowing user intent
		DataSet<Tuple3<Long, Double, Double>> joinTrafiicRon = traffic.join(ron).where(0).equalTo(0).projectFirst(0, 1).projectSecond(1);

		// Build big join table
		DataSet<Tuple4<Long, Double, Double, Double>> joinTrafiicRonUI = joinTrafiicRon.join(userIntent).where(0).equalTo(0).projectFirst(0, 1, 2)
				.projectSecond(1);

		/*
		 * DataSet<Tuple4<Long, Double, Double, Double>>
		 * joinTrafiicRonUIResource =
		 * joinTrafiicRonUI.join(normalizedResource).where(0).equalTo(0)
		 * .projectFirst(0, 1, 2, 3).projectSecond(1);
		 */

		DataSet<Tuple4<String, Double, Double, Double>> joinAllAndName = joinTrafiicRonUI.join(companyIndex).where(0).equalTo(1).projectSecond(0)
				.projectFirst(1, 2, 3);

		joinAllAndName.writeAsCsv(argPathFactors, WriteMode.OVERWRITE);

		env.execute();

	}	

}
