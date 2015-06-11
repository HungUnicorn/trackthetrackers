package io.ssc.trackthetrackers.analysis.runofnetwork;

import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Pattern;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

// Get weighted PageRank for undirected weighted graph
// Due to undirected, the graph is already an ergodic Markov Chain
// Weight presents the probability to transfer from one node to the other node
// The process runs as: 1. Normalize weight to make it become probability (edge weight / sum of weight in a node)
// 2. Distributed rank based on weight rather than degree
// Ref:Weighted PageRank: cluster-related weights by Danil Nemirovskya and Konstantin Avrachenkovb

public class WeightedPageRankMain implements ProgramDescription {

	private static String argPathToWeightedEdges = "/home/sendoh/datasets/UndirectedWeighetedGraphTwoPhase";
	private static String argPathOut = Config.get("analysis.results.path")
			+ "TopWeightedPageRank";

	private static double DAMPENING_FACTOR = 0.85;
	private static int maxIterations = 20;

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		

		DataSet<Tuple3<Long, Long, Double>> weightedEdges = ReaderUtils.readWeightedEdges(env, argPathToWeightedEdges);				

		// Get sum of edge weight for each node
		DataSet<Tuple2<Long, Double>> sumEdgeWeight = weightedEdges.groupBy(0)
				.reduceGroup(new SourceSumEdgeWeight());

		// Normalization:sum of weight is equals to one
		DataSet<Tuple3<Long, Long, Double>> normalizedEdgeWeight = weightedEdges.map(
				new WeightNormalized()).withBroadcastSet(sumEdgeWeight,
				"sumEdgeWeight");

		DataSet<Tuple1<Long>> nodes = weightedEdges.<Tuple1<Long>> project(0)
				.distinct();

		// Get the total count of nodes
		DataSet<Long> numNodes = nodes.reduceGroup(new CountNodes());

		// Initial rank : 1 / numNodes
		DataSet<Tuple2<Long, Double>> pagesRanked = nodes.map(
				new InitialRanking()).withBroadcastSet(numNodes, "numNodes");

		Graph<Long, Double, Double> network = Graph.fromTupleDataSet(
				pagesRanked, normalizedEdgeWeight, env);

		DataSet<Vertex<Long, Double>> pageRanks = network.run(
				new WeightedPageRank<Long>(nodes.count(), DAMPENING_FACTOR,
						maxIterations)).getVertices();

		pageRanks.writeAsCsv(argPathOut, WriteMode.OVERWRITE);
		
		env.execute();
	}	

	// Sum the weight of edges
	public static class SourceSumEdgeWeight
			implements
			GroupReduceFunction<Tuple3<Long, Long, Double>, Tuple2<Long, Double>> {
		@Override
		public void reduce(Iterable<Tuple3<Long, Long, Double>> edges,
				Collector<Tuple2<Long, Double>> collector) throws Exception {
			Double sumWeight = 0.0;
			Iterator<Tuple3<Long, Long, Double>> iterator = edges.iterator();

			while (iterator.hasNext()) {
				Tuple3<Long, Long, Double> edge = iterator.next();
				Long node = edge.f0;
				sumWeight += edge.f2;
				collector.collect(new Tuple2<Long, Double>(node, sumWeight));
			}
		}
	}

	// Use Log?
	public static class WeightNormalized
			extends
			RichMapFunction<Tuple3<Long, Long, Double>, Tuple3<Long, Long, Double>> {
		private HashMap<Long, Double> sumEdgeWeightMap = new HashMap<Long, Double>();

		@Override
		public void open(Configuration parameters) throws Exception {

			ArrayList<Tuple2<Long, Double>> sumEdgeWeight = (ArrayList) getRuntimeContext()
					.getBroadcastVariable("sumEdgeWeight");

			for (Tuple2<Long, Double> node : sumEdgeWeight) {
				sumEdgeWeightMap.put(node.f0, node.f1);
			}

		}

		@Override
		public Tuple3<Long, Long, Double> map(Tuple3<Long, Long, Double> edge)
				throws Exception {
			Long node = edge.f0;
			Long connectNode = edge.f1;
			Double edgeWeight = edge.f2;
			Double sumEdgeWeight = sumEdgeWeightMap.get(node);
			Double newEdgeWeight = edgeWeight / sumEdgeWeight;
			return new Tuple3<Long, Long, Double>(node, connectNode,
					newEdgeWeight);
		}
	}

	public static class CountNodes implements
			GroupReduceFunction<Tuple1<Long>, Long> {
		@Override
		public void reduce(Iterable<Tuple1<Long>> pages,
				Collector<Long> collector) throws Exception {
			collector.collect(new Long(Iterables.size(pages)));
		}
	}

	public static class InitialRanking extends
			RichMapFunction<Tuple1<Long>, Tuple2<Long, Double>> {
		private long numNodes = 0L;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			numNodes = getRuntimeContext().<Long> getBroadcastVariable(
					"numNodes").get(0);
		}

		@Override
		public Tuple2<Long, Double> map(Tuple1<Long> node) throws Exception {
			return new Tuple2<Long, Double>(node.f0, 1.0d / numNodes);
		}
	}	

	@Override
	public String getDescription() {
		return "WeightedPageRank";
	}
}