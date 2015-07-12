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

	private static int topK = 10000;

	private static String argPathToIndex = "/home/sendoh/datasets/companyIndex.tsv";
	private static String argPathToNodeAndValue = "/home/sendoh/datasets/UserIntent/userIntent.csv";
	private static String argPathOut = "/home/sendoh/datasets/UserIntent/topIntent.csv";

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<String, Long>> nodes = ReaderUtils.readNameWithCommaAndId(env, argPathToIndex);

		/* Convert the input as (node, value) */
		DataSet<Tuple2<Long, Double>> nodesAndValue = ReaderUtils.readLongAndValue(env, argPathToNodeAndValue);

		DataSet<Tuple3<Long, Long, Double>> topKMapper = nodesAndValue.flatMap(new TopKMapper());

		// Get topK
		DataSet<Tuple3<Long, Long, Double>> topKReducer = topKMapper.groupBy(0).sortGroup(2, Order.DESCENDING).first(topK);

		// Node ID joins with node's name
		DataSet<Tuple2<String, Double>> topKwithName = topKReducer.join(nodes).where(1).equalTo(1).projectSecond(0).projectFirst(2);

		topKwithName.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();
	}

	public static class TopKMapper implements FlatMapFunction<Tuple2<Long, Double>, Tuple3<Long, Long, Double>> {

		@Override
		public void flatMap(Tuple2<Long, Double> tuple, Collector<Tuple3<Long, Long, Double>> collector) throws Exception {
			collector.collect(new Tuple3<Long, Long, Double>((long) 1, tuple.f0, tuple.f1));
		}
	}
}
