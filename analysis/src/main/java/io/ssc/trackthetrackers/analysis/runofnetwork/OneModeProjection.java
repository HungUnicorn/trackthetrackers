package io.ssc.trackthetrackers.analysis.runofnetwork;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;
import io.ssc.trackthetrackers.analysis.runofnetwork.ResourceAllocation.GetNeighbors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

/* Get undirected weighted graph from bipartite graph (one-mode projection)
 Edge exists if two third parties embed the same first party significantly
 Use significance to decrease the connectivity
 This uses Sign test significance because the distribution is absolutely not a normal distribution 
 Critical region: Z(0.05) * (n/4)^1/2 + 2/n (From moment generating function)

 reference: "A systematic approach to the one-mode projection of bipartite graphs"

 i.e. Project a bipartite graph (X, Y) onto  
 (Xi, Xj, Weight)

 Weight definition: The resource-allocation 
 Reference: "Bipartite network projection and personal recommendation"
 */

public class OneModeProjection {

	private static String argPathToEmbedssArcs = Config.get("analysis.results.path") + "distinctArcCompanyLevel";

	private static String argPathNodeResource = Config.get("analysis.results.path") + "nodeResource";
	private static String argPathOut = Config.get("analysis.results.path") + "undirectedWeighetedGraph";

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> arcs = ReaderUtils.readArcs(env, argPathToEmbedssArcs);

		// Get node resource
		DataSet<Tuple2<Long, Double>> nodeResource = ReaderUtils.readLongAndValue(env, argPathNodeResource);

		// Neighbors of Y
		DataSet<Tuple2<Long, Long[]>> nodesYWithNeighbors = arcs.<Tuple2<Long, Long>> project(1, 0).groupBy(0).reduceGroup(new GetNeighbors());

		// Generate edge based on weight compute before
		DataSet<Tuple3<Long, Long, Double>> edgesWithWeight = nodesYWithNeighbors.map(new AddEdgeIfSignificant()).withBroadcastSet(nodeResource,
				"nodeWithWeight");

		DataSet<Tuple3<Long, Long, Double>> nonNullEdge = edgesWithWeight.filter(new FilterNullEdge());

		DataSet<Tuple3<Long, Long, Double>> undirectedWeightedGraph = nonNullEdge.groupBy(0, 1).aggregate(Aggregations.SUM, 2);

		undirectedWeightedGraph.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

	}

	public static class FilterNullEdge implements FilterFunction<Tuple3<Long, Long, Double>> {

		@Override
		public boolean filter(Tuple3<Long, Long, Double> edge) throws Exception {
			return edge != null;
		}
	}

	public static class GenerateWeight extends RichMapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Double>> {

		HashMap<Long, Double> nodeWeightMap = new HashMap<Long, Double>();

		@Override
		public void open(Configuration parameters) throws Exception {

			ArrayList<Tuple2<Long, Double>> nodeWithWeight = (ArrayList) getRuntimeContext().getBroadcastVariable("nodeWithWeight");

			for (Tuple2<Long, Double> node : nodeWithWeight) {
				Long id = node.f0;
				Double weight = node.f1;
				nodeWeightMap.put(id, weight);
			}
		}

		@Override
		public Tuple3<Long, Long, Double> map(Tuple2<Long, Long> edge) throws Exception {
			Long node = edge.f0;
			Long connectNode = edge.f1;
			Double nodeWeight = nodeWeightMap.get(node);
			Double connectNodeWeight = nodeWeightMap.get(connectNode);
			Double edgeWeight = nodeWeight + connectNodeWeight;

			return new Tuple3<Long, Long, Double>(node, connectNode, edgeWeight);
		}
	}

	public static class AddEdgeIfSignificant extends RichMapFunction<Tuple2<Long, Long[]>, Tuple3<Long, Long, Double>> {

		HashMap<Long, Double> nodeWeightMap = new HashMap<Long, Double>();
		// Critical region: Z(0.05) * (n/4)^1/2 + 2/n (From moment generating
		// function)
		// Edge weight = node1 weight + node2 weight, so size * 2
		double criticalRegion = 1.645 * Math.sqrt(2 * nodeWeightMap.size() / 4) + (2 / 2 * nodeWeightMap.size());

		@Override
		public void open(Configuration parameters) throws Exception {

			ArrayList<Tuple2<Long, Double>> nodeWithWeight = (ArrayList) getRuntimeContext().getBroadcastVariable("nodeWithWeight");

			for (Tuple2<Long, Double> node : nodeWithWeight) {
				Long id = node.f0;
				Double weight = node.f1;
				nodeWeightMap.put(id, weight);
			}
		}

		@Override
		public Tuple3<Long, Long, Double> map(Tuple2<Long, Long[]> nodeWithNeighbors) throws Exception {
			Long[] neighbors = nodeWithNeighbors.f1;

			// Each two neighbors have one arc
			for (int i = 0; i < neighbors.length; i++) {
				Long node = nodeWithNeighbors.f1[i];
				for (int j = 0; j < neighbors.length; j++) {
					// Use one direction to represent undirected arc and
					// eliminate self
					Long connectNode = nodeWithNeighbors.f1[j];
					if (i != j && i < j) {
						Double nodeWeight = nodeWeightMap.get(node);
						Double connectNodeWeight = nodeWeightMap.get(connectNode);
						Double edgeWeight = nodeWeight + connectNodeWeight;
						if (edgeWeight > criticalRegion) {
							return new Tuple3<Long, Long, Double>(node, connectNode, edgeWeight);
						}
					}
				}
			}
			return null;
		}
	}

	// Get neighbors of node via ArrayList
	public static class GetNeighbors implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> iterable, Collector<Tuple2<Long, Long[]>> collector) throws Exception {
			long id = 0l;
			ArrayList<Long> neighborsList = new ArrayList<Long>();

			Iterator<Tuple2<Long, Long>> iterator = iterable.iterator();
			while (iterator.hasNext()) {
				Tuple2<Long, Long> tuple = iterator.next();
				id = tuple.f0;
				neighborsList.add(tuple.f1);
			}
			collector.collect(new Tuple2<Long, Long[]>(id, neighborsList.toArray(new Long[neighborsList.size()])));
		}
	}

	public static class DistributeWeight implements FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple3<Long, Long, Double>> {

		@Override
		public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value, Collector<Tuple3<Long, Long, Double>> collector)
				throws Exception {

			Long node = value.f0.f0;
			Long[] neighbors = value.f1.f1;
			Double newWeight = value.f0.f1 / neighbors.length;
			for (int i = 0; i < neighbors.length; i++)
				collector.collect(new Tuple3<Long, Long, Double>(node, neighbors[i], newWeight));

		}
	}

	public static class ProjectJoinArcs implements FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

		@Override
		public void flatMap(Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> arcs, Collector<Tuple2<Long, Long>> collector) throws Exception {
			Long node = arcs.f0.f0;
			Long connectNode = arcs.f1.f0;
			if (node < connectNode) {
				collector.collect(new Tuple2<Long, Long>(node, connectNode));
			}
		}
	}

	public static class InitialWeight implements FlatMapFunction<Tuple2<Long, Long[]>, Tuple2<Long, Double>> {

		@Override
		public void flatMap(Tuple2<Long, Long[]> nodesWithNeighbors, Collector<Tuple2<Long, Double>> collector) throws Exception {
			Long id = nodesWithNeighbors.f0;
			Double initialWeight = 1.0;
			collector.collect(new Tuple2<Long, Double>(id, initialWeight));
		}
	}

}
