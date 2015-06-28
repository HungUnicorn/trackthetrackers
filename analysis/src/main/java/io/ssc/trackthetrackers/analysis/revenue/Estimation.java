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
public class Estimation {

	private static String argPathUserIntent = "/home/sendoh/datasets/UserIntent/userIntent";
	private static String argPathTaffic = "/home/sendoh/datasets/Traffic/trafficCompany_pr";
	private static String argPathRON = "/home/sendoh/datasets/RON/weightedPageRank";
	private static String argPathNodeResource = "/home/sendoh/datasets/RON/nodeResource";
	private static String argPathCompanyIndex = "/home/sendoh/datasets/companyIndex.tsv";

	private static String argPathFactors = Config.get("analysis.results.path") + "/Revenue/" + "factors.csv";
	private static String argPathRevenueWithoutUserIntent = Config.get("analysis.results.path") + "/Revenue/" + "revenueWithoutUserIntent.csv";
	private static String argPathRevenue = Config.get("analysis.results.path") + "/Revenue/" + "revenue.csv";		

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Double>> userIntent = ReaderUtils.readLongAndValue(env, argPathUserIntent);
		DataSet<Tuple2<Long, Double>> traffic = ReaderUtils.readLongAndValue(env, argPathTaffic);
		DataSet<Tuple2<Long, Double>> ron = ReaderUtils.readLongAndValue(env, argPathRON);
		DataSet<Tuple2<Long, Double>> resource = ReaderUtils.readLongAndValue(env, argPathNodeResource);

		DataSet<Tuple2<String, Long>> companyIndex = ReaderUtils.readNameWithCommaAndId(env, argPathCompanyIndex);

		// Min-Max normalizatioin
		DataSet<Tuple2<Long, Double>> userIntentMax = userIntent.maxBy(1);
		DataSet<Tuple2<Long, Double>> userIntentMin = userIntent.minBy(1);
		DataSet<Tuple2<Long, Double>> normalizedUserIntent = userIntent.map(new MinMaxNormalization()).withBroadcastSet(userIntentMax, "max")
				.withBroadcastSet(userIntentMin, "min");
		
		DataSet<Tuple2<Long, Double>> traffictMax = traffic.maxBy(1);
		DataSet<Tuple2<Long, Double>> traffictMin = traffic.minBy(1);
		DataSet<Tuple2<Long, Double>> normalizedTraffic = traffic.map(new MinMaxNormalization()).withBroadcastSet(traffictMax, "max")
				.withBroadcastSet(traffictMin, "min");

		DataSet<Tuple2<Long, Double>> ronMax = ron.maxBy(1);
		DataSet<Tuple2<Long, Double>> ronMin = ron.minBy(1);
		DataSet<Tuple2<Long, Double>> normalizedRon = ron.map(new MinMaxNormalization()).withBroadcastSet(ronMax, "max")
				.withBroadcastSet(ronMin, "min");

		DataSet<Tuple2<Long, Double>> resourceMax = resource.maxBy(1);
		DataSet<Tuple2<Long, Double>> resourceMin = resource.minBy(1);
		DataSet<Tuple2<Long, Double>> normalizedResource = resource.map(new MinMaxNormalization()).withBroadcastSet(resourceMax, "max")
				.withBroadcastSet(resourceMin, "min");

		// W/o knowing user intent
		DataSet<Tuple3<Long, Double, Double>> joinTrafficRon = normalizedTraffic.join(normalizedRon).where(0).equalTo(0).projectFirst(0, 1)
				.projectSecond(1);

		// Build big join table
		DataSet<Tuple4<Long, Double, Double, Double>> joinTrafficRonUI = joinTrafficRon.join(normalizedUserIntent).where(0).equalTo(0)
				.projectFirst(0, 1, 2).projectSecond(1);

		DataSet<Tuple4<Long, Double, Double, Double>> joinTrafficRonUIResource = joinTrafficRonUI.join(normalizedResource).where(0).equalTo(0)
				.projectFirst(0, 1, 2, 3).projectSecond(1);

		// Name, tra
		DataSet<Tuple4<String, Double, Double, Double>> joinAllAndName = joinTrafficRonUIResource.join(companyIndex).where(0).equalTo(1)
				.projectSecond(0).projectFirst(1, 2, 3, 4);

		// Revenue estimation
		DataSet<Tuple2<String, Double>> companyRevenue = joinTrafficRonUI.flatMap(new RevenueEstimation()).join(companyIndex).where(0).equalTo(1)
				.projectSecond(0).projectFirst(1);

		// Revenue estimation W/o user intent
		DataSet<Tuple2<String, Double>> RevenueWithoutUserIntent = joinTrafficRon.flatMap(new RevenueWithoutUserIntent()).join(companyIndex).where(0)
				.equalTo(1).projectSecond(0).projectFirst(1);

		joinAllAndName.writeAsCsv(argPathFactors, WriteMode.OVERWRITE);
		companyRevenue.writeAsCsv(argPathRevenue, WriteMode.OVERWRITE);
		RevenueWithoutUserIntent.writeAsCsv(argPathRevenueWithoutUserIntent, WriteMode.OVERWRITE);

		env.execute();

	}

	public static class RevenueWithoutUserIntent implements FlatMapFunction<Tuple3<Long, Double, Double>, Tuple2<Long, Double>> {

		@Override
		public void flatMap(Tuple3<Long, Double, Double> value, Collector<Tuple2<Long, Double>> collector) throws Exception {

			double revenue = value.f1 * value.f2;
			collector.collect(new Tuple2<Long, Double>(value.f0, revenue));

		}
	}

	public static class RevenueEstimation implements FlatMapFunction<Tuple4<Long, Double, Double, Double>, Tuple2<Long, Double>> {

		@Override
		public void flatMap(Tuple4<Long, Double, Double, Double> value, Collector<Tuple2<Long, Double>> collector) throws Exception {

			double revenue = value.f1 * value.f2 * value.f3;
			collector.collect(new Tuple2<Long, Double>(value.f0, revenue));

		}
	}

	/*
	 * Min-max normalization x = 1 + (x - min) / (max - min) ~ [1,2], +1
	 * facilitates multiplication of other factors
	 */
	public static class MinMaxNormalization extends RichMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

		private static double min;
		private static double max;

		@Override
		public void open(Configuration parameters) throws Exception {
			ArrayList<Tuple2<Long, Double>> minArr = (ArrayList) getRuntimeContext().getBroadcastVariable("min");
			ArrayList<Tuple2<Long, Double>> maxArr = (ArrayList) getRuntimeContext().getBroadcastVariable("max");
			min = minArr.get(0).f1;
			max = maxArr.get(0).f1;
			System.out.println("Min:" + min);
			System.out.println("Max:" + max);
			
		}

		@Override
		public Tuple2<Long, Double> map(Tuple2<Long, Double> value) throws Exception {
			double valueTransformed = 1 + ((value.f1 - min) / (max - min));
			return new Tuple2<Long, Double>(value.f0, valueTransformed);
		}
	}
}
