package io.ssc.trackthetrackers.analysis.etl;

import io.ssc.trackthetrackers.Config;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

//Generate (Third party, pld index of first party) file
// Filter the remained columns

public class ProjectThirdPartyAndFirstParty {
	private static String argPathTrackingArc = Config
			.get("analysis.results.path") + "quarter.tsv";

	private static String argPathOut = Config.get("analysis.results.path")
			+ "filteredQuarter.csv";

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputArcs = env.readTextFile(argPathTrackingArc);

		DataSet<Tuple2<String, Long>> arcs = inputArcs.flatMap(new ArcReader());

		arcs.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

	}

	public static class ArcReader implements
			FlatMapFunction<String, Tuple2<String, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t]");

		@Override
		public void flatMap(String s, Collector<Tuple2<String, Long>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				//System.out.println("normal String " + s);
				// a bad string �4��b>��1����#�Uw�bsMQ�����е��|�G=n�����N8��i���
				if (tokens.length < 2) {
					return ;
				}
				String thirdParty = tokens[1];
				long firstParty = Long.parseLong(tokens[0]);
				collector.collect(new Tuple2<String, Long>(thirdParty,
						firstParty));
			}
		}
	}

}
