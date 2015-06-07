/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Hung Chang
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.ssc.trackthetrackers.analysis.webtraffic;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Aggregate Centrality to company level
public class TrafficThirdPartyCompany {
	private static String argPathThirdPartyTraffic = Config.get("analysis.results.path") + "trafficDotComArc";
	private static String argPathToDomainCompany = "/home/sendoh/trackthetrackers/analysis/src/resources/company/domainCompanyMapping";
	private static String argPathToThirdPartyIndex = Config.get("analysis.results.path") + "thirdPartyIndex";
	private static String argPathToCompanyIndex = Config.get("analysis.results.path") + "companyIndex.tsv";

	private static String argPathOut = Config.get("analysis.results.path") + "trafficCompany";

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Double>> thirdPartyTraffic = ReaderUtils.readLongAndValue(env, argPathThirdPartyTraffic);

		// Domain, Company
		DataSet<Tuple2<String, String>> domainCompany = ReaderUtils.readDomainCompanySymbol(env, argPathToDomainCompany);

		// Name, Id
		DataSet<Tuple2<String, Long>> thirdPartyIndex = ReaderUtils.readNameAndId(env, argPathToThirdPartyIndex);

		// Name, Value
		DataSet<Tuple2<String, Double>> nameAndTraffic = thirdPartyTraffic.join(thirdPartyIndex).where(0).equalTo(1).projectSecond(0).projectFirst(1);

		// Join company and domain
		// Company, Value
		DataSet<Tuple2<String, Double>> companyTrafficPreAgg = nameAndTraffic.join(domainCompany).where(0).equalTo(0).projectSecond(1)
				.projectFirst(1);

		DataSet<Tuple2<String, Double>> companyTraffic = companyTrafficPreAgg.groupBy(0).aggregate(Aggregations.SUM, 1);

		// NameWithComma, Id
		DataSet<Tuple2<String, Long>> companyIndex = ReaderUtils.readNameWithComma(env, argPathToCompanyIndex);

		// Filter company
		DataSet<Tuple2<String,Double>> strictCompanyTraffic = companyIndex.join(companyTraffic).where(0).equalTo(0).projectSecond(0).projectSecond(1);
		
		strictCompanyTraffic.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

	}
}
