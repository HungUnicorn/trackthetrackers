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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Get the top K third party.
public class TopTrafficCompany {
	private static String centrality = "pr";
	// private static String measures = "h";
	private static String argPathTrafficCompany = Config.get("analysis.results.path") + "trafficCompany_" + centrality;
	private static String argPathToCompanyIndex = Config.get("analysis.results.path") + "companyIndex.tsv";

	private static String argPathOut = Config.get("analysis.results.path") + "topTrafficCompany_" + centrality;

	private static int topK = 10000;

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Convert the input as (nodeName, value)
		DataSet<Tuple2<Long, Double>> embedIDAndValue = ReaderUtils.readLongAndValue(env, argPathTrafficCompany);

		// Output 1, ID, value
		DataSet<Tuple3<Long, Long, Double>> topKMapper = embedIDAndValue.flatMap(new TopKMapper());

		// Get topK
		DataSet<Tuple3<Long, Long, Double>> topKReducer = topKMapper.groupBy(0).sortGroup(2, Order.DESCENDING).first(topK);

		DataSet<Tuple2<String, Long>> companyIndex = ReaderUtils.readNameWithCommaAndId(env, argPathToCompanyIndex);

		// Node ID joins with node's name
		DataSet<Tuple2<String, Double>> topKwithName = topKReducer.join(companyIndex).where(1).equalTo(1).projectSecond(0).projectFirst(2);

		topKwithName.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

	}

	public static class TopKMapper implements FlatMapFunction<Tuple2<Long, Double>, Tuple3<Long, Long, Double>> {

		@Override
		public void flatMap(Tuple2<Long, Double> tuple, Collector<Tuple3<Long, Long, Double>> collector) throws Exception {
			collector.collect(new Tuple3<Long, Long, Double>((long) 1, tuple.f0, tuple.f1));
		}
	}
}
