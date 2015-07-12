package io.ssc.trackthetrackers.analysis.revenue;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem.WriteMode;

// Get the big join table consists of all factors
public class JoinFactors {

	private static String argPathUserIntentBreadth = "/home/sendoh/datasets/UserIntent/userIntentBreadth.csv";
	private static String argPathUserIntentDepth = "/home/sendoh/datasets/UserIntent/userIntentDepth.csv";
	private static String argPathPrivacyHazard = "/home/sendoh/datasets/UserIntent/privacyHazard.csv";

	private static String argPathTafficPR = "/home/sendoh/datasets/Traffic/trafficCompany_pr";
	private static String argPathTafficH = "/home/sendoh/datasets/Traffic/trafficCompany_h";

	private static String argPathWPR = "/home/sendoh/datasets/RON/weightedPageRank";
	private static String argPathNodeResource = "/home/sendoh/datasets/RON/nodeResource";

	private static String argPathCompanyIndex = "/home/sendoh/datasets/companyIndex.tsv";

	private static String argPathFactors = Config.get("analysis.results.path") + "/Revenue/" + "joinFactors.csv";

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Double>> userIntentBreadth = ReaderUtils.readLongAndValue(env, argPathUserIntentBreadth);
		DataSet<Tuple2<Long, Double>> userIntentDepth = ReaderUtils.readLongAndValue(env, argPathUserIntentDepth);
		DataSet<Tuple2<Long, Double>> privacyHazard = ReaderUtils.readLongAndValue(env, argPathPrivacyHazard);

		DataSet<Tuple2<Long, Double>> trafficPR = ReaderUtils.readLongAndValue(env, argPathTafficPR);
		DataSet<Tuple2<Long, Double>> trafficH = ReaderUtils.readLongAndValue(env, argPathTafficH);

		DataSet<Tuple2<Long, Double>> wpr = ReaderUtils.readLongAndValue(env, argPathWPR);
		DataSet<Tuple2<Long, Double>> nodeResource = ReaderUtils.readLongAndValue(env, argPathNodeResource);

		DataSet<Tuple2<String, Long>> companyIndex = ReaderUtils.readNameWithCommaAndId(env, argPathCompanyIndex);

		// Traffic
		DataSet<Tuple3<Long, Double, Double>> joinTraffic = trafficPR.join(trafficH).where(0).equalTo(0).projectFirst(0, 1).projectSecond(1);

		// User Intent
		DataSet<Tuple3<Long, Double, Double>> joinUserIntent = userIntentBreadth.join(userIntentDepth).where(0).equalTo(0).projectFirst(0, 1)
				.projectSecond(1);
		DataSet<Tuple4<Long, Double, Double, Double>> joinUIandPrivacy = joinUserIntent.join(privacyHazard).where(0).equalTo(0).projectFirst(0, 1, 2)
				.projectSecond(1);

		// RON
		DataSet<Tuple3<Long, Double, Double>> ron = wpr.join(nodeResource).where(0).equalTo(0).projectFirst(0, 1).projectSecond(1);

		// All factors
		DataSet<Tuple5<Long, Double, Double, Double, Double>> joinTrafficRon = joinTraffic.join(ron).where(0).equalTo(0).projectFirst(0, 1, 2)
				.projectSecond(1, 2);

		DataSet<Tuple8<Long, Double, Double, Double, Double, Double, Double, Double>> joinTrafiicRonUI = joinTrafficRon.join(joinUIandPrivacy)
				.where(0).equalTo(0).projectFirst(0, 1, 2, 3, 4).projectSecond(1, 2, 3);

		DataSet<Tuple8<String, Double, Double, Double, Double, Double, Double, Double>> joinAllAndName = joinTrafiicRonUI.join(companyIndex).where(0)
				.equalTo(1).projectSecond(0).projectFirst(1, 2, 3, 4, 5, 6, 7);

		joinAllAndName.writeAsCsv(argPathFactors, WriteMode.OVERWRITE);

		env.execute();

	}

}
