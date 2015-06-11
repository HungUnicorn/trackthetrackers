package io.ssc.trackthetrackers.analysis.runofnetwork;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

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

/* Get the weight in undirected graph from bipartite graph (one-mode projection)
 Weight definition: The resource-allocation
 process consists of two steps; first X distribute weight to Y (X-Y), then Y sent it back to
 X. Compute weight based on the number of neighbors of X and Y (similar to PageRank).
 e.g. First, X has three neighbors so it distributes (1/3) *X to Y. 
 Second, Y has two neighbors so it send (1/3) * X * (1/2) Y to X. All nodes in X and Y follow this rule     
 Reference:"Bipartite network projection and personal recommendation"
 */

public class ResourceAllocation {

	private static String argPathToEmbedArcs = Config.get("analysis.results.path") + "distinctArcCompanyLevel";
	private static String argPathNodeAndWeight = Config.get("analysis.results.path") + "nodeResource";

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> arcs = ReaderUtils.readArcs(env, argPathToEmbedArcs);

		// Get neighbors of nodes of X
		DataSet<Tuple2<Long, Long[]>> nodesXWithNeighbors = arcs.groupBy(0).reduceGroup(new GetNeighbors());

		// Neighbors of Y
		DataSet<Tuple2<Long, Long[]>> nodesYWithNeighbors = arcs.<Tuple2<Long, Long>> project(1, 0).groupBy(0).reduceGroup(new GetNeighbors());

		// Get initial weight of X
		DataSet<Tuple2<Long, Double>> nodeXWithInitialWeight = nodesXWithNeighbors.flatMap(new InitialWeight());

		// First Step: X distribute weight to Y
		// (X,Y,weight)
		DataSet<Tuple3<Long, Long, Double>> firstDistribute = nodeXWithInitialWeight.join(nodesXWithNeighbors).where(0).equalTo(0)
				.flatMap(new DistributeWeight());

		DataSet<Tuple2<Long, Double>> afterFirstDistribute = firstDistribute.groupBy(1).aggregate(Aggregations.SUM, 2)
				.<Tuple2<Long, Double>> project(1, 2);

		// Second Step : Y distribute weight to X
		// (Y,X,weight)
		DataSet<Tuple3<Long, Long, Double>> secondDistritbute = afterFirstDistribute.join(nodesYWithNeighbors).where(0).equalTo(0)
				.flatMap(new DistributeWeight());

		DataSet<Tuple2<Long, Double>> nodeResource = secondDistritbute.groupBy(1).aggregate(Aggregations.SUM, 2).<Tuple2<Long, Double>> project(1, 2);

		nodeResource.writeAsCsv(argPathNodeAndWeight, WriteMode.OVERWRITE);

		env.execute();

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

	public static class InitialWeight implements FlatMapFunction<Tuple2<Long, Long[]>, Tuple2<Long, Double>> {

		@Override
		public void flatMap(Tuple2<Long, Long[]> nodesWithNeighbors, Collector<Tuple2<Long, Double>> collector) throws Exception {
			Long id = nodesWithNeighbors.f0;
			Double initialWeight = 1.0;
			collector.collect(new Tuple2<Long, Double>(id, initialWeight));
		}
	}

}
