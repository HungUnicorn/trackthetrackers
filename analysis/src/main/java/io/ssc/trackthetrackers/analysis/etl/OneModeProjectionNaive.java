package io.ssc.trackthetrackers.analysis.etl;

import io.ssc.trackthetrackers.Config;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
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
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

/* Get undirected weighted graph from bipartite graph (one-mode projection)
 Edge exists if two third parties track the same first party,
 Project a bipartite graph (X, Y) onto  
 (Xi, Xj, Weight) 
 (Node, ConnectNode, Weight)
 Naive weight def: the amount of tracking the same first party.
 */

public class OneModeProjectionNaive {

	// private static String argPathToTrackingArcs = Config
	// .get("analysis.trackingraphsample.path");

	private static String argPathToTrackingArcs = "/home/sendoh/datasets/projection/sample";

	private static String argPathOut = Config.get("analysis.results.path")
			+ "UndirectedWeighetedGraph";

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputTrackingArcs = env
				.readTextFile(argPathToTrackingArcs);

		DataSet<Tuple2<Long, Long>> trackingArcs = inputTrackingArcs
				.flatMap(new ArcReader());				

		// Get neighbors of nodes
		DataSet<Tuple2<Long, Long[]>> nodesYWithNeighbors = trackingArcs
				.<Tuple2<Long, Long>> project(1, 0).groupBy(0)
				.reduceGroup(new GetNeighbors());

		DataSet<Tuple2<Long, Long>> edges = nodesYWithNeighbors
				.flatMap(new AddEdgeIfSharingSameNode());

		DataSet<Tuple3<Long,Long,Long>> undirectedEdges = edges.flatMap(new ArcOneMapper()).groupBy(0,1).aggregate(Aggregations.SUM, 2);
		
		undirectedEdges.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

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
					// Use one direction to represent undirected arc and eliminate self
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

	public static class ArcOneMapper implements
			FlatMapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>> {

		@Override
		public void flatMap(Tuple2<Long, Long> arc,
				Collector<Tuple3<Long, Long, Long>> collector) throws Exception {
			collector.collect(new Tuple3<Long, Long, Long>(arc.f0, arc.f1, 1L));
		}
	}

}
