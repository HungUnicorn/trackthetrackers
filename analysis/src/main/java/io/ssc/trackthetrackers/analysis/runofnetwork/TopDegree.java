package io.ssc.trackthetrackers.analysis.runofnetwork;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

import java.util.Iterator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Get the top generalizing degree in undirected weighted graph.
// Generalized degree = alpha * weight + (1-alpha)* # neighbors
// Ref: http://toreopsahl.com/2010/04/21/article-node-centrality-in-weighted-networks-generalizing-degree-and-shortest-paths/

public class TopDegree {

	//private static String argPathToPLD = Config.get("webdatacommons.pldfile.unzipped");
	private static String argPathToCompanyIndex = Config.get("analysis.results.path") + "companyIndex.tsv";
	
	private static String argPathToUndirectWeightedGraph = "/home/sendoh/datasets/undirectedWeighetedGraph";
	
	private static String argPathOut = Config.get("analysis.results.path")
			+ "TopDegreeUWG";

	private static int degreeFilter = 50;
	private static int topK = 30;

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		
		DataSet<Tuple2<String, Long>> companyIndex = ReaderUtils.readNameWithCommaAndId(env, argPathToCompanyIndex);		

		DataSet<Tuple3<Long, Long, Double>> weightedEdges = ReaderUtils.readWeightedEdges(env, argPathToUndirectWeightedGraph);

		// Compute the degree of every vertex
		DataSet<Tuple2<Long, Double>> verticesWithGeneralizingDegree = weightedEdges
				.<Tuple2<Long, Double>> project(1, 2).groupBy(0)
				.reduceGroup(new GeneralizingDegree ());

		// Focus on the nodes' degree higher than certain degree
		DataSet<Tuple2<Long, Double>> highGeneralizingDegreeDegree = verticesWithGeneralizingDegree
				.filter(new DegreeFilter());

		// Output 1, ID, value
		DataSet<Tuple3<Long, Long, Double>> topKMapper = highGeneralizingDegreeDegree
				.flatMap(new TopKMapper());

		// Get topK
		DataSet<Tuple3<Long, Long, Double>> topKReducer = topKMapper.groupBy(0)
				.sortGroup(2, Order.DESCENDING).first(topK);

		// Node ID joins with node's name
		DataSet<Tuple2<String, Double>> topKwithName = topKReducer
				.join(companyIndex).where(1).equalTo(1)
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

	public static class GeneralizingDegree implements
			GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {
		@Override
		public void reduce(Iterable<Tuple2<Long, Double>> tuples,
				Collector<Tuple2<Long, Double>> collector) throws Exception {

			Iterator<Tuple2<Long, Double>> iterator = tuples.iterator();
			Long vertexId = iterator.next().f0;
			// Consider #Ties also
			Long count = 0l;
			Double weight = (double) 0;
			while (iterator.hasNext()) {
				Tuple2<Long, Double> neighbor = iterator.next();
				Double neighborWeight = neighbor.f1;
				weight += neighborWeight;
				count++;
			}
			Double totalWeight = 0.5 * weight + 0.5 * count;

			collector.collect(new Tuple2<Long, Double>(vertexId, totalWeight));
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
