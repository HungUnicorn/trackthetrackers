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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Get the aggregated Centrality distritbution
public class TrafficThirdParty {

	private static String argPathToNodesAndValues = Config.get("webdatacommons.hostgraph-pr.unzipped");
	private static String argPathToPLD = Config.get("webdatacommons.pldfile.unzipped");
	// private static String argPathToTrackingArcs =
	// Config.get("analysis.results.path") + "filterNonBusinessArc";
	private static String argPathToTrackingArcs = Config.get("analysis.results.path") + "filterDotComArc";
	private static String argPathOut = Config.get("analysis.results.path") + "trafficDotComArc";

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> trackingArcs = ReaderUtils.readArcs(env, argPathToTrackingArcs);

		// Convert the input as (nodeName, value)
		DataSet<Tuple2<String, Double>> nodeAndValue = ReaderUtils.readStringAndValue(env, argPathToNodesAndValues);

		// Read PLD index
		DataSet<Tuple2<String, Long>> pldNodes = ReaderUtils.readNameAndId(env, argPathToPLD);

		// Get first party ID and value (ID, value)
		DataSet<Tuple2<Long, Double>> NodeIdAndValue = nodeAndValue.join(pldNodes).where(0).equalTo(0).projectSecond(1).projectFirst(1);

		// Get (3rd partty, 1st party, value of 1st party)
		DataSet<Tuple2<Long, Double>> arcsValues = trackingArcs.join(NodeIdAndValue).where(1).equalTo(0).projectFirst(0).projectSecond(1);

		DataSet<Tuple2<Long, Double>> trackerIDAndValue = arcsValues.groupBy(0).aggregate(Aggregations.SUM, 1);

		trackerIDAndValue.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

	}
}
