package io.ssc.trackthetrackers.analysis;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class ReaderUtils {

	public static DataSet<Tuple2<String, Long>> readNameAndId(ExecutionEnvironment env, String filePath) {
		DataSource<String> inputNameAndId = env.readTextFile(filePath);
		DataSet<Tuple2<String, Long>> nameAndId = inputNameAndId.flatMap(new NameAndIdReader());
		return nameAndId;

	}

	public static DataSet<Tuple3<Long, Long, Double>> readWeightedEdges(ExecutionEnvironment env, String filePath) {
		DataSource<String> inputWeightedEdges = env.readTextFile(filePath);
		DataSet<Tuple3<Long, Long, Double>> weightedEdges = inputWeightedEdges.flatMap(new WeightedEdgeReader());
		return weightedEdges;

	}

	public static DataSet<Tuple2<Long, Long>> readArcs(ExecutionEnvironment env, String filePath) {
		DataSource<String> inputArc = env.readTextFile(filePath);
		DataSet<Tuple2<Long, Long>> arcs = inputArc.flatMap(new ArcReader());
		return arcs;

	}

	public static DataSet<Tuple2<String, Long>> readStringArcs(ExecutionEnvironment env, String filePath) {
		DataSource<String> inputArc = env.readTextFile(filePath);
		DataSet<Tuple2<String, Long>> arcs = inputArc.flatMap(new StringArcReader());
		return arcs;

	}

	public static DataSet<Tuple2<String, Double>> readStringAndValue(ExecutionEnvironment env, String filePath) {
		DataSource<String> inputNodeValue = env.readTextFile(filePath);
		DataSet<Tuple2<String, Double>> nodeValue = inputNodeValue.flatMap(new StringAndValueReader());
		return nodeValue;

	}

	public static DataSet<Tuple2<Long, Double>> readLongAndValue(ExecutionEnvironment env, String filePath) {
		DataSource<String> inputNodeValue = env.readTextFile(filePath);
		DataSet<Tuple2<Long, Double>> nodeValue = inputNodeValue.flatMap(new LongAndValueReader());
		return nodeValue;

	}

	public static DataSet<Tuple2<String, String>> readDomainCompanyFullName(ExecutionEnvironment env, String filePath) {
		DataSource<String> inputDomainCompany = env.readTextFile(filePath);
		DataSet<Tuple2<String, String>> domainAndCompany = inputDomainCompany.flatMap(new CompanyFullNameReader());
		return domainAndCompany;

	}

	public static DataSet<Tuple2<String, String>> readDomainCompanySymbol(ExecutionEnvironment env, String filePath) {
		DataSource<String> inputDomainCompany = env.readTextFile(filePath);
		DataSet<Tuple2<String, String>> domainAndCompany = inputDomainCompany.flatMap(new CompanySymbolReader());
		return domainAndCompany;

	}

	public static DataSet<Tuple2<String, Long>> readNameWithCommaAndId(ExecutionEnvironment env, String filePath) {
		DataSource<String> inputTsv = env.readTextFile(filePath);
		DataSet<Tuple2<String, Long>> NamdWithCommaAndId = inputTsv.flatMap(new NameWithCommaAndIdReader());
		return NamdWithCommaAndId;

	}

	public static class NameAndIdReader implements FlatMapFunction<String, Tuple2<String, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				String node = tokens[0];
				long nodeIndex = Long.parseLong(tokens[1]);
				collector.collect(new Tuple2<String, Long>(node, nodeIndex));
			}
		}
	}

	public static class WeightedEdgeReader implements FlatMapFunction<String, Tuple3<Long, Long, Double>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple3<Long, Long, Double>> collector) throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);
				Double weight = Double.parseDouble(tokens[2]);

				// Undirected graph
				collector.collect(new Tuple3<Long, Long, Double>(source, target, weight));
				collector.collect(new Tuple3<Long, Long, Double>(target, source, weight));
			}
		}
	}

	public static class ArcReader implements FlatMapFunction<String, Tuple2<Long, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<Long, Long>> collector) throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);
				collector.collect(new Tuple2<Long, Long>(source, target));
			}
		}
	}

	public static class StringArcReader implements FlatMapFunction<String, Tuple2<String, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[\t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				String source = tokens[0];
				long target = Long.parseLong(tokens[1]);
				collector.collect(new Tuple2<String, Long>(source, target));
			}
		}
	}

	public static class StringAndValueReader implements FlatMapFunction<String, Tuple2<String, Double>> {

		private static final Pattern SEPARATOR = Pattern.compile("[\t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<String, Double>> collector) throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				String node = tokens[0];
				double value = Double.parseDouble(tokens[1]);
				collector.collect(new Tuple2<String, Double>(node, value));
			}
		}
	}

	public static class LongAndValueReader implements FlatMapFunction<String, Tuple2<Long, Double>> {

		private static final Pattern SEPARATOR = Pattern.compile("[\t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<Long, Double>> collector) throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				Long node = Long.parseLong(tokens[0]);
				double value = Double.parseDouble(tokens[1]);
				collector.collect(new Tuple2<Long, Double>(node, value));
			}
		}
	}

	// Keep domains' full name
	public static class CompanyFullNameReader implements FlatMapFunction<String, Tuple2<String, String>> {

		@Override
		public void flatMap(String input, Collector<Tuple2<String, String>> collector) throws Exception {
			if (!input.startsWith("%")) {
				String domain = input.substring(0, input.indexOf(","));
				String company = input.substring(input.indexOf(",") + 1).trim();
				String companyCommaRemoved = CompanyParser.readCompanyCommaRemoved(company);
				collector.collect(new Tuple2<String, String>(domain, companyCommaRemoved));
			}
		}
	}

	// Symbol only
	public static class CompanySymbolReader implements FlatMapFunction<String, Tuple2<String, String>> {

		private static final Pattern SEPARATOR = Pattern.compile("[\t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<String, String>> collector) throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				String domain = tokens[0];
				String company = tokens[1];
				collector.collect(new Tuple2<String, String>(domain, company));
			}
		}
	}

	// Company name has , e.g. facebook, Inc.
	public static class NameWithCommaAndIdReader implements FlatMapFunction<String, Tuple2<String, Long>> {

		@Override
		public void flatMap(String input, Collector<Tuple2<String, Long>> collector) throws Exception {
			String name = input.substring(0, input.lastIndexOf(","));
			String id = input.substring(input.indexOf(",") + 1).trim();

			String nameCommaRemoved = CompanyParser.readCompanyCommaRemoved(name);
			Long longId = Long.parseLong(id);
			collector.collect(new Tuple2<String, Long>(nameCommaRemoved, longId));

		}
	}
}
