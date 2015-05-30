package io.ssc.trackthetrackers.analysis.extraction.category;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

public class CheckWithPLDIndex {

	private static String argPathToPLD = Config
			.get("webdatacommons.pldfile.unzipped");

	private static String argPathToCategorySite = Config
			.get("analysis.results.path") + "allCategorySites";

	private static String argPathOut = Config.get("analysis.results.path")
			+ "TopCategorySitePLD";

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSet<Tuple2<String, Long>> pldIndex = ReaderUtils.readPldIndex(env,
				argPathToPLD);

		DataSource<String> inputCategorySite = env
				.readTextFile(argPathToCategorySite);

		DataSet<Tuple2<String, String>> categorySite = inputCategorySite
				.flatMap(new CategoorySiteReader());

		DataSet<Tuple2<Long, String>> pldIdCategorySite = categorySite
				.join(pldIndex).where(0).equalTo(0)
				.flatMap(new ProjectPldCategoryWithId());

		DataSet<Tuple2<String, String>> pldNameCategorySite = categorySite
				.join(pldIndex).where(0).equalTo(0)
				.flatMap(new ProjectPldCategoryWithName());

		pldNameCategorySite.writeAsCsv(argPathOut);
		
		env.execute();

	}

	public static class CategoorySiteReader implements
			FlatMapFunction<String, Tuple2<String, String>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s,
				Collector<Tuple2<String, String>> collector) throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				String site = tokens[0];
				String category = tokens[1];
				collector.collect(new Tuple2<String, String>(site, category));
			}
		}
	}

	public static class ProjectPldCategoryWithId
			implements
			FlatMapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>, Tuple2<Long, String>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<String, String>, Tuple2<String, Long>> joinTuple,
				Collector<Tuple2<Long, String>> collector) throws Exception {
			Long pldIndex = joinTuple.f1.f1;
			String category = joinTuple.f0.f1;
			collector.collect(new Tuple2<Long, String>(pldIndex, category));

		}
	}

	public static class ProjectPldCategoryWithName
			implements
			FlatMapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>, Tuple2<String, String>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<String, String>, Tuple2<String, Long>> joinTuple,
				Collector<Tuple2<String, String>> collector) throws Exception {
			String site = joinTuple.f0.f0;
			String category = joinTuple.f0.f1;
			collector.collect(new Tuple2<String, String>(site, category));

		}
	}
}
