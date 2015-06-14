package io.ssc.trackthetrackers.analysis.userintent;

import io.ssc.trackthetrackers.analysis.ReaderUtils;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

/*TopK: a general class to get TopK nodes with names 
 * Input should be CSV(writeAsCsv) but not txt(writeAsTxt)
 * */

public class TopKName {

	private static int topK = 40;

	private static String argPathToIndex = "/home/sendoh/datasets/companyIndex.tsv";
	private static String argPathToNodesAndValues = "/home/sendoh/datasets/userIntent";
	private static String argPathOut = "/home/sendoh/datasets/topUserIntent";

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<String, Long>> nodes = ReaderUtils.readNameWithCommaAndId(env, argPathToIndex);

		/* Convert the input as (node, value) */
		DataSet<Tuple2<Long, Double>> nodesAndValue = ReaderUtils.readLongAndValue(env, argPathToNodesAndValues);

		DataSet<Tuple3<Long, Long, Double>> topKMapper = nodesAndValue.flatMap(new TopKMapper());

		// Get topK
		DataSet<Tuple3<Long, Long, Double>> topKReducer = topKMapper.groupBy(0).sortGroup(2, Order.DESCENDING).first(topK);

		// Node ID joins with node's name
		DataSet<Tuple2<String, Double>> topKwithName = topKReducer.join(nodes).where(1).equalTo(1).flatMap(new ProjectNodeWithName());

		topKwithName.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();
	}

	public static class ProjectNodeWithName implements
			FlatMapFunction<Tuple2<Tuple3<Long, Long, Double>, Tuple2<String, Long>>, Tuple2<String, Double>> {

		@Override
		public void flatMap(Tuple2<Tuple3<Long, Long, Double>, Tuple2<String, Long>> value, Collector<Tuple2<String, Double>> collector)
				throws Exception {

			collector.collect(new Tuple2<String, Double>(value.f1.f0, value.f0.f2));

		}
	}

	public static class TopKMapper implements FlatMapFunction<Tuple2<Long, Double>, Tuple3<Long, Long, Double>> {

		@Override
		public void flatMap(Tuple2<Long, Double> tuple, Collector<Tuple3<Long, Long, Double>> collector) throws Exception {
			collector.collect(new Tuple3<Long, Long, Double>((long) 1, tuple.f0, tuple.f1));
		}
	}
}
