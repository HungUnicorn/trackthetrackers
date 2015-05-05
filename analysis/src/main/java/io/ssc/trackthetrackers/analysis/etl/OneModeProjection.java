package io.ssc.trackthetrackers.analysis.etl;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.etl.OneModeProjectionNaive.AddEdgeIfSharingSameNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

/* Get undirected weighted graph from bipartite graph (one-mode projection)
 Edge exists if two third parties track the same first party,
 Project a bipartite graph (X, Y) onto  
 (Xi, Xj, Weight)

 Weight definition: The resource-allocation
 process consists of two steps; first X distribute weight to Y (X-Y), then Y sent it back to
 X. Compute weight based on the number of neighbors of X and Y (similar to PageRank).
 e.g. First, X has three neighbors so it distributes (1/3) *X to Y. 
 Second, Y has two neighbors so it send (1/3) * X * (1/2) Y to X. All nodes in X and Y follow this rule     
 reference:"Bipartite network projection and personal recommendation"
 */

public class OneModeProjection {

	 private static String argPathToTrackingArcs = Config.get("analysis.trackingraphsample.path");

	//private static String argPathToTrackingArcs = "/home/sendoh/datasets/projection/sample";

	private static String argPathOut = Config.get("analysis.results.path")
			+ "UndirectedWeighetedGraphTwoPhase";

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputTrackingArcs = env
				.readTextFile(argPathToTrackingArcs);

		DataSet<Tuple2<Long, Long>> trackingArcs = inputTrackingArcs
				.flatMap(new ArcReader());

		// Get neighbors of nodes of X
		DataSet<Tuple2<Long, Long[]>> nodesXWithNeighbors = trackingArcs
				.groupBy(0).reduceGroup(new GetNeighbors());

		// Neighbors of Y
		DataSet<Tuple2<Long, Long[]>> nodesYWithNeighbors = trackingArcs
				.<Tuple2<Long, Long>> project(1, 0).groupBy(0)
				.reduceGroup(new GetNeighbors());

		// Get initial weight of X
		DataSet<Tuple2<Long, Double>> nodeXWithInitialWeight = nodesXWithNeighbors
				.flatMap(new InitialWeight());

		// First Step: X distribute weight to Y
		// (X,Y,weight)
		DataSet<Tuple3<Long, Long, Double>> firstDistribute = nodeXWithInitialWeight
				.join(nodesXWithNeighbors).where(0).equalTo(0)
				.flatMap(new DistributeWeight());

		DataSet<Tuple2<Long, Double>> afterFirstDistribute = firstDistribute
				.groupBy(1).aggregate(Aggregations.SUM, 2)
				.<Tuple2<Long, Double>> project(1, 2);

		// Second Step : Y distribute weight to X
		// (Y,X,weight)
		DataSet<Tuple3<Long, Long, Double>> secondDistritbute = afterFirstDistribute
				.join(nodesYWithNeighbors).where(0).equalTo(0)
				.flatMap(new DistributeWeight());

		DataSet<Tuple2<Long, Double>> nodeWithWeight = secondDistritbute
				.groupBy(1).aggregate(Aggregations.SUM, 2)
				.<Tuple2<Long, Double>> project(1, 2);		
		
		// Generate edge based on weight compute before
		DataSet<Tuple2<Long, Long>> edges = nodesYWithNeighbors.flatMap(
				new AddEdgeIfSharingSameNode()).distinct();

		DataSet<Tuple3<Long, Long, Double>> edgesWithWeight = edges.map(
				new GenerateWeight()).withBroadcastSet(nodeWithWeight,
				"nodeWithWeight");

		edgesWithWeight.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

	}

	public static class GenerateWeight extends
			RichMapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Double>> {

		HashMap<Long, Double> nodeWeightMap = new HashMap<Long, Double>();

		@Override
		public void open(Configuration parameters) throws Exception {

			ArrayList<Tuple2<Long, Double>> nodeWithWeight = (ArrayList) getRuntimeContext()
					.getBroadcastVariable("nodeWithWeight");

			for (Tuple2<Long, Double> node : nodeWithWeight) {
				Long id = node.f0;
				Double weight = node.f1;
				nodeWeightMap.put(id, weight);
			}
		}

		@Override
		public Tuple3<Long, Long, Double> map(Tuple2<Long, Long> edge)
				throws Exception {
			Long node = edge.f0;
			Long connectNode = edge.f1;
			Double nodeWeight = nodeWeightMap.get(node);
			Double connectNodeWeight = nodeWeightMap.get(connectNode);
			Double edgeWeight = nodeWeight + connectNodeWeight;

			return new Tuple3<Long, Long, Double>(node, connectNode, edgeWeight);
		}
	}

	public static class AddEdgeIfSharingSameNode implements
			FlatMapFunction<Tuple2<Long, Long[]>, Tuple2<Long, Long>> {

		@Override
		public void flatMap(Tuple2<Long, Long[]> nodeWithNeighbors,
				Collector<Tuple2<Long, Long>> collector) throws Exception {

			Long[] neighbors = nodeWithNeighbors.f1;

			ArrayList<Tuple2<Long, Long>> arcs = new ArrayList<Tuple2<Long, Long>>();

			// Each two neighbors have one arc
			for (int i = 0; i < neighbors.length; i++) {
				for (int j = 0; j < neighbors.length; j++) {
					// Use one direction to represent undirected arc and
					// eliminate self
					if (i != j && i < j) {
						arcs.add(new Tuple2<Long, Long>(neighbors[i],
								neighbors[j]));
					}
				}
			}
			for (Tuple2<Long, Long> arc : arcs) {
				collector.collect(arc);
			}

		}
	}

	// Get neighbors of node via ArrayList
	public static class GetNeighbors implements
			GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> iterable,
				Collector<Tuple2<Long, Long[]>> collector) throws Exception {
			long id = 0l;
			ArrayList<Long> neighborsList = new ArrayList<Long>();

			Iterator<Tuple2<Long, Long>> iterator = iterable.iterator();
			while (iterator.hasNext()) {
				Tuple2<Long, Long> tuple = iterator.next();
				id = tuple.f0;
				neighborsList.add(tuple.f1);
			}
			collector.collect(new Tuple2<Long, Long[]>(id, neighborsList
					.toArray(new Long[neighborsList.size()])));
		}
	}

	public static class DistributeWeight
			implements
			FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple3<Long, Long, Double>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value,
				Collector<Tuple3<Long, Long, Double>> collector)
				throws Exception {

			Long node = value.f0.f0;
			Long[] neighbors = value.f1.f1;
			Double newWeight = value.f0.f1 / neighbors.length;
			for (int i = 0; i < neighbors.length; i++)
				collector.collect(new Tuple3<Long, Long, Double>(node,
						neighbors[i], newWeight));

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

	public static class ProjectJoinArcs
			implements
			FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> arcs,
				Collector<Tuple2<Long, Long>> collector) throws Exception {
			Long node = arcs.f0.f0;
			Long connectNode = arcs.f1.f0;
			if (node < connectNode) {
				collector.collect(new Tuple2<Long, Long>(node, connectNode));
			}
		}
	}

	public static class InitialWeight implements
			FlatMapFunction<Tuple2<Long, Long[]>, Tuple2<Long, Double>> {

		@Override
		public void flatMap(Tuple2<Long, Long[]> nodesWithNeighbors,
				Collector<Tuple2<Long, Double>> collector) throws Exception {
			Long id = nodesWithNeighbors.f0;
			Double initialWeight = 1.0;
			collector.collect(new Tuple2<Long, Double>(id, initialWeight));
		}
	}

}
