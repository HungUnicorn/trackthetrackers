package io.ssc.trackthetrackers.analysis.etl;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Arc(String, Long) -> (Long, Long)

public class StringArcToLongArc {
	private static String argPathTrackingArc = Config.get("analysis.results.path") + "filteredQuarter.csv";

	private static String argPathThirdPartyIndex = Config.get("analysis.results.path") + "thirdPartyIndex";

	private static String argPathOut = Config.get("analysis.results.path") + "longArc";

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<String, Long>> stringArcs = ReaderUtils.readStringArcs(env, argPathTrackingArc);

		DataSet<Tuple2<String, Long>> nodes = ReaderUtils.readPldIndex(env, argPathThirdPartyIndex);

		DataSet<Tuple2<Long,Long>> longArcs = stringArcs.joinWithTiny(nodes).where(0).equalTo(0).flatMap(new ProjectLongArc());
		longArcs.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

	}

	public static class ProjectLongArc implements FlatMapFunction<Tuple2<Tuple2<String, Long>, Tuple2<String, Long>>, Tuple2<Long, Long>> {

		@Override
		public void flatMap(Tuple2<Tuple2<String, Long>, Tuple2<String, Long>> joinTuple, Collector<Tuple2<Long, Long>> collector) throws Exception {
			Long thirdParty = joinTuple.f1.f1;
			Long firstParty = joinTuple.f0.f1;
			collector.collect(new Tuple2<Long, Long>(thirdParty, firstParty));
		}
	}
}
