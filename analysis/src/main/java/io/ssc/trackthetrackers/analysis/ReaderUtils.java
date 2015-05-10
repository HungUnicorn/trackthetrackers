package io.ssc.trackthetrackers.analysis;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class ReaderUtils {

	public static DataSet<Tuple2<String, Long>> readCompanyIndex(
			ExecutionEnvironment env, String filePath) {
		DataSource<String> inputCompanyIndex = env.readTextFile(filePath);
		DataSet<Tuple2<String, Long>> companyIndex = inputCompanyIndex
				.flatMap(new CompanyIndexReader());
		return companyIndex;

	}

	public static DataSet<Tuple2<String, Long>> readPldIndex(
			ExecutionEnvironment env, String filePath) {
		DataSource<String> inputPldIndex = env.readTextFile(filePath);
		DataSet<Tuple2<String, Long>> pldIndex = inputPldIndex
				.flatMap(new PldIndexReader());
		return pldIndex;

	}

	public static DataSet<Tuple3<Long, Long, Double>> readWeightedEdges(
			ExecutionEnvironment env, String filePath) {
		DataSource<String> inputWeightedEdges = env.readTextFile(filePath);
		DataSet<Tuple3<Long, Long, Double>> weightedEdges = inputWeightedEdges
				.flatMap(new WeightedEdgeReader());
		return weightedEdges;

	}
	
	public static DataSet<Tuple2<Long, Long>> readArcs(
			ExecutionEnvironment env, String filePath) {
		DataSource<String> inputArc = env.readTextFile(filePath);
		DataSet<Tuple2<Long, Long>> arcs = inputArc
				.flatMap(new ArcReader());
		return arcs;

	}

	// Company index reader
	public static class CompanyIndexReader implements
			FlatMapFunction<String, Tuple2<String, Long>> {

		@Override
		public void flatMap(String input,
				Collector<Tuple2<String, Long>> collector) throws Exception {
			if (!input.startsWith("%")) {
				String company = input.substring(0, input.indexOf(",")).trim();
				Long companyIndex = Long.parseLong(input.substring(input
						.indexOf(",") + 1));
				collector.collect(new Tuple2<String, Long>(company,
						companyIndex));
			}
		}
	}

	// Pld index reader
	public static class PldIndexReader implements
			FlatMapFunction<String, Tuple2<String, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<String, Long>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				String node = tokens[0];
				long nodeIndex = Long.parseLong(tokens[1]);
				collector.collect(new Tuple2<String, Long>(node, nodeIndex));
			}
		}
	}

	public static class WeightedEdgeReader implements
			FlatMapFunction<String, Tuple3<Long, Long, Double>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s,
				Collector<Tuple3<Long, Long, Double>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);
				Double weight = Double.parseDouble(tokens[2]);

				// Undirected graph
				collector.collect(new Tuple3<Long, Long, Double>(source,
						target, weight));
				collector.collect(new Tuple3<Long, Long, Double>(target,
						source, weight));
			}
		}
	}

	public static class ArcReader implements
			FlatMapFunction<String, Tuple2<Long, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<Long, Long>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);
				collector.collect(new Tuple2<Long, Long>(source, target));
			}
		}
	}
}
