package io.ssc.trackthetrackers.analysis.undirected;

import io.ssc.trackthetrackers.Config;

import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Get the top degree in undirected weighted graph.
public class TopDegree {

	private static String argPathToPLD = Config
			.get("webdatacommons.pldfile.unzipped");
	private static String argPathToUndirectWeightedGraph = "/home/sendoh/datasets/UndirectedWeighetedGraphTwoPhase";
	private static String argPathOut = Config.get("analysis.results.path")
			+ "TopDegreeUndirect";

	private static int degreeFilter = 100;
	private static int topK = 20;

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputEdges = env
				.readTextFile(argPathToUndirectWeightedGraph);

		DataSource<String> inputNodePLD = env.readTextFile(argPathToPLD);

		DataSet<Tuple2<String, Long>> pldNodes = inputNodePLD
				.flatMap(new NodeReader());

		DataSet<Tuple3<Long, Long, Double>> edges = inputEdges
				.flatMap(new EdgeReader());

		// Compute the degree of every vertex
		DataSet<Tuple2<Long, Double>> verticesWithDegree = edges
				.<Tuple2<Long, Double>> project(1, 2).groupBy(0)
				.reduceGroup(new DegreeOfVertex());

		// Focus on the nodes' degree higher than certain degree
		DataSet<Tuple2<Long, Double>> highDegree = verticesWithDegree
				.filter(new DegreeFilter());

		// Output 1, ID, value
		DataSet<Tuple3<Long, Long, Double>> topKMapper = highDegree
				.flatMap(new TopKMapper());

		// Get topK
		DataSet<Tuple3<Long, Long, Double>> topKReducer = topKMapper.groupBy(0)
				.sortGroup(2, Order.DESCENDING).first(topK);

		// Node ID joins with node's name
		DataSet<Tuple2<String, Double>> topKwithName = topKReducer
				.join(pldNodes).where(1).equalTo(1)
				.flatMap(new ProjectIDWithName());

		topKwithName.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

	}

	public static class DegreeFilter implements
			FilterFunction<Tuple2<Long, Double>> {

		@Override
		public boolean filter(Tuple2<Long, Double> value) throws Exception {
			return value.f1 > degreeFilter;
		}
	}

	public static class DegreeOfVertex implements
			GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {
		@Override
		public void reduce(Iterable<Tuple2<Long, Double>> tuples,
				Collector<Tuple2<Long, Double>> collector) throws Exception {

			Iterator<Tuple2<Long, Double>> iterator = tuples.iterator();
			Long vertexId = iterator.next().f0;

			Double weight = (double) 0;
			while (iterator.hasNext()) {
				Tuple2<Long, Double> neighbor = iterator.next();
				Double neighborWeight = neighbor.f1;
				weight += neighborWeight;
			}

			collector.collect(new Tuple2<Long, Double>(vertexId, weight));
		}
	}

	public static class NodeReader implements
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

	public static class EdgeReader implements
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
				collector.collect(new Tuple3<Long, Long, Double>(source,
						target, weight));
			}
		}
	}

	public static class TopKMapper implements
			FlatMapFunction<Tuple2<Long, Double>, Tuple3<Long, Long, Double>> {

		@Override
		public void flatMap(Tuple2<Long, Double> tuple,
				Collector<Tuple3<Long, Long, Double>> collector)
				throws Exception {
			collector.collect(new Tuple3<Long, Long, Double>((long) 1,
					tuple.f0, tuple.f1));
		}
	}

	public static class ProjectIDWithName
			implements
			FlatMapFunction<Tuple2<Tuple3<Long, Long, Double>, Tuple2<String, Long>>, Tuple2<String, Double>> {

		@Override
		public void flatMap(
				Tuple2<Tuple3<Long, Long, Double>, Tuple2<String, Long>> value,
				Collector<Tuple2<String, Double>> collector) throws Exception {
			collector.collect(new Tuple2<String, Double>(value.f1.f0,
					value.f0.f2));
		}
	}
}
