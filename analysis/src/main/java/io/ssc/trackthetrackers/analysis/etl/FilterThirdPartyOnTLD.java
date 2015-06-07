package io.ssc.trackthetrackers.analysis.etl;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;

// Filter the third party based on TLD. Those remains in first party

public class FilterThirdPartyOnTLD {
	private static String argPathTrackingArc = Config.get("analysis.results.path") + "longArc";

	private static String argPathIndex = Config.get("analysis.results.path") + "thirdPartyIndex";

	private static String argPathOut = Config.get("analysis.results.path") + "filterDotComArc";

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> arcs = ReaderUtils.readArcs(env, argPathTrackingArc);

		DataSet<Tuple2<String, Long>> index = ReaderUtils.readNameAndId(env, argPathIndex);

		DataSet<Tuple2<String, Long>> arcsWithThirdPartyName = arcs.join(index).where(0).equalTo(1).projectSecond(0).projectFirst(1);

		DataSet<Tuple2<String, Long>> filterArcs = arcsWithThirdPartyName.filter(new DotComFilter());

		DataSet<Tuple2<Long, Long>> filterLongArcs = filterArcs.join(index).where(0).equalTo(0).projectSecond(1).projectFirst(1);

		filterLongArcs.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();
	}

	public static class DomainFilter implements FilterFunction<Tuple2<String, Long>> {

		@Override
		public boolean filter(Tuple2<String, Long> arc) throws Exception {
			String domain = arc.f0;
			Pattern SEPARATOR = Pattern.compile("[.]");
			String tokens[] = SEPARATOR.split(domain);

			// Ignore the domain name for example, .com, .gov
			if (tokens.length < 2) {
				return false;
			}

			String tld = tokens[tokens.length - 1];
			String potentialTld = tokens[tokens.length - 2];

			// TLD
			if (!tld.equalsIgnoreCase("gov") && !tld.equalsIgnoreCase("edu") && !tld.equalsIgnoreCase("mil")) {
				// ccTLD e.g. aa.gov.tw
				if (!potentialTld.equalsIgnoreCase("gov") && !potentialTld.equalsIgnoreCase("edu") && !potentialTld.equalsIgnoreCase("mil")) {
					return true;

				}
			}

			return false;
		}
	}

	public static class DotComFilter implements FilterFunction<Tuple2<String, Long>> {

		@Override
		public boolean filter(Tuple2<String, Long> arc) throws Exception {
			String domain = arc.f0;

			String tld = domain.substring(domain.lastIndexOf(".") + 1).trim().toLowerCase();			

			// TLD
			if (tld.equalsIgnoreCase("com") || tld.equalsIgnoreCase("net") || tld.equalsIgnoreCase("org")) {
				return true;
			}

			return false;
		}
	}
}
