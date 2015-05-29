package io.ssc.trackthetrackers.analysis.etl;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

//Generate distinct third party, using Set directly will be out of heap space  

public class DistinctThridParty {
	private static String argPathTrackingArc = Config.get("analysis.results.path") + "filteredQuarter.csv";

	private static String argPathOut = Config.get("analysis.results.path") + "distinctThirdParty.csv";

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<String, Long>> arcs = ReaderUtils.readStringArcs(env, argPathTrackingArc);

		DataSet<Tuple1<String>> distinctThirdParty = arcs.<Tuple1<String>> project(0).distinct();

		distinctThirdParty.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();
	}
}
