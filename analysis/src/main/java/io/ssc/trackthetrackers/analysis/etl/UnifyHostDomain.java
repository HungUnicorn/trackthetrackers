package io.ssc.trackthetrackers.analysis.etl;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.DomainParser;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Transform all third party in to xxx.com because google lib views xxx.blogspot.com and yyy.blogspot as different which is incorrect
// xxx.blogspot and yyy.blogspot embeds in the same first party means blogspot can see this
// After aggregating to host domain(blogspot), just left one tuple rather than two tuple 
// Viewing as two tuple is overestimated

public class UnifyHostDomain {
	private static String argPathEmbedArc = Config.get("analysis.results.path") + "filterBusinessArc";

	private static String argPathOut = Config.get("analysis.results.path") + "unifyArc";

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<String, Long>> arcs = ReaderUtils.readStringArcs(env, argPathEmbedArc);

		DataSet<Tuple2<String, Long>> unifiedArcs = arcs.flatMap(new UnifyHostDomainMapper());

		DataSet<Tuple2<String, Long>> unifiedArcsDistinct = unifiedArcs.distinct();

		unifiedArcsDistinct.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();
	}

	public static class UnifyHostDomainMapper implements FlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[.]");

		@Override
		public void flatMap(Tuple2<String, Long> arc, Collector<Tuple2<String, Long>> collector) throws Exception {
			String domain = arc.f0;
			String tokens[] = SEPARATOR.split(domain);
			String thirdParty = null;

			if (tokens.length < 2) {
				return;
			}

			if (tokens.length > 2) {
				String tld = DomainParser.getTLD(domain);
				String host = tokens[tokens.length - 2];
				thirdParty = host + "." + tld;
			} else {
				thirdParty = domain;
			}

			long firstParty = arc.f1;
			collector.collect(new Tuple2<String, Long>(thirdParty, firstParty));

		}
	}

}
