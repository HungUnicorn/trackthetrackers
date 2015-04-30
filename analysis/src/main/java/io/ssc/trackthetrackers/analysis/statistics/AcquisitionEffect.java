package io.ssc.trackthetrackers.analysis.statistics;

import io.ssc.trackthetrackers.Config;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.util.Collector;

/* Check the overlap coverage of acquisition
 * http://en.wikipedia.org/wiki/List_of_mergers_and_acquisitions_by_Facebook
 * Facebook bought Instgram at 2012
 */
public class AcquisitionEffect {

	// Need more acquisition data
	private static String parent = "Facebook";
	private static String child = "Instagram";

	private static String argPathToDomainCompany = "/home/sendoh/trackthetrackers/analysis/src/resources/company/DomainAndCompany.csv";
	private static String argPathToPLD = Config
			.get("webdatacommons.pldfile.unzipped");
	private static String argPathToTrackingArcs = Config
			.get("analysis.trackingraphsample.path");

	private static String argPathOverlap = Config.get("analysis.results.path")
			+ "Acquisition/" + "Overlap";

	private static String argPathOut = Config.get("analysis.results.path")
			+ "Acquisition/" + "percent";

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputTrackingArcs = env
				.readTextFile(argPathToTrackingArcs);

		DataSource<String> inputDomainCompany = env
				.readTextFile(argPathToDomainCompany);

		DataSource<String> inputNodePLD = env.readTextFile(argPathToPLD);

		// Domain, Company
		DataSet<Tuple2<String, String>> domainCompany = inputDomainCompany
				.flatMap(new DomainReader());

		DataSet<Tuple2<Long, Long>> trackingArcs = inputTrackingArcs
				.flatMap(new ArcReader());

		DataSet<Tuple2<String, Long>> pldNodes = inputNodePLD
				.flatMap(new NodeReader());

		DataSet<Tuple2<String, Long>> arcsWithSrcName = trackingArcs
				.join(pldNodes).where(0).equalTo(1)
				.flatMap(new ProjectSrcNameAndDes());

		DataSet<Tuple3<String, String, Long>> arcsWithCompanyAndSrcName = arcsWithSrcName
				.join(domainCompany).where(0).equalTo(0)
				.flatMap(new ProjectSrcComanyAndDes());

		// Get arcs of designated parent and child
		DataSet<Tuple3<String, String, Long>> companySpecificArcs = arcsWithCompanyAndSrcName
				.filter(new CompanyFilter());

		DataSet<Tuple3<String, String, Long>> parentArcs = companySpecificArcs
				.filter(new ParentFilter());

		DataSet<Long> numParentTrack = parentArcs.<Tuple1<Long>> project(2)
				.reduceGroup(new CountTracked());

		DataSet<Tuple3<String, String, Long>> childArcs = companySpecificArcs
				.filter(new ChildFilter());

		DataSet<Long> numChildTrack = childArcs.<Tuple1<Long>> project(2)
				.reduceGroup(new CountTracked());

		DataSet<Tuple3<String, String, Long>> overlapChildAndParent = parentArcs
				.join(childArcs).where(2).equalTo(2)
				.flatMap(new ProjectOverlapArc());

		// Add Sum of Centrality factor
		 
		DataSet<Long> numOverlap = overlapChildAndParent
				.<Tuple1<Long>> project(2).reduceGroup(new CountTracked());

		DataSet<Double> percentOverlap = numOverlap.map(new DivideBySum())
				.withBroadcastSet(numParentTrack, "numParentTrack")
				.withBroadcastSet(numChildTrack, "numChildTrack");
			
		overlapChildAndParent.writeAsCsv(argPathOverlap, WriteMode.OVERWRITE);

		percentOverlap.print();

		env.execute();

	}

	public static class ProjectOverlapArc
			implements
			FlatMapFunction<Tuple2<Tuple3<String, String, Long>, Tuple3<String, String, Long>>, Tuple3<String, String, Long>> {

		@Override
		public void flatMap(
				Tuple2<Tuple3<String, String, Long>, Tuple3<String, String, Long>> joinTuple,
				Collector<Tuple3<String, String, Long>> collector)
				throws Exception {

			String company = joinTuple.f0.f0;
			String srcName = joinTuple.f0.f1;
			Long des = joinTuple.f0.f2;

			collector.collect(new Tuple3<String, String, Long>(company,
					srcName, des));
		}

	}

	public static class DivideBySum extends RichMapFunction<Long, Double> {

		private Long numParentTrack;
		private Long numChildTrack;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			numParentTrack = getRuntimeContext().<Long> getBroadcastVariable(
					"numParentTrack").get(0);
			numChildTrack = getRuntimeContext().<Long> getBroadcastVariable(
					"numChildTrack").get(0);

			System.out.println(numParentTrack);
			System.out.println(numChildTrack);
		}

		@Override
		public Double map(Long value) throws Exception {
			return ((double) value / (numParentTrack + numChildTrack));
		}

	}

	public static class CountTracked implements
			GroupReduceFunction<Tuple1<Long>, Long> {

		@Override
		public void reduce(Iterable<Tuple1<Long>> arc, Collector<Long> out)
				throws Exception {
			out.collect(new Long(Iterables.size(arc)));

		}

	}

	public static class ChildFilter implements
			FilterFunction<Tuple3<String, String, Long>> {

		@Override
		public boolean filter(Tuple3<String, String, Long> arc)
				throws Exception {

			String company = arc.f0;
			if (company.contains(child)) {
				return true;
			}
			return false;
		}

	}

	public static class ParentFilter implements
			FilterFunction<Tuple3<String, String, Long>> {

		@Override
		public boolean filter(Tuple3<String, String, Long> arc)
				throws Exception {

			String company = arc.f0;
			if (company.contains(parent)) {
				return true;
			}
			return false;
		}

	}

	public static class ProjectSrcComanyAndDes
			implements
			FlatMapFunction<Tuple2<Tuple2<String, Long>, Tuple2<String, String>>, Tuple3<String, String, Long>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<String, Long>, Tuple2<String, String>> joinTuple,
				Collector<Tuple3<String, String, Long>> collector)
				throws Exception {
			String company = joinTuple.f1.f1;
			String srcName = joinTuple.f0.f0;
			Long des = joinTuple.f0.f1;
			collector.collect(new Tuple3<String, String, Long>(company,
					srcName, des));
		}

	}

	public static class ProjectSrcNameAndDes
			implements
			FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<String, Long>>, Tuple2<String, Long>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Long>, Tuple2<String, Long>> joinTuple,
				Collector<Tuple2<String, Long>> collector) throws Exception {
			String srcName = joinTuple.f1.f0;
			Long des = joinTuple.f0.f1;
			collector.collect(new Tuple2<String, Long>(srcName, des));
		}

	}

	public static class CompanyFilter implements
			FilterFunction<Tuple3<String, String, Long>> {

		@Override
		public boolean filter(Tuple3<String, String, Long> arc)
				throws Exception {

			String company = arc.f0;
			if (company.contains(parent) || company.contains(child)) {
				return true;
			}
			return false;
		}

	}

	// Symbol only
	public static class DomainReader implements
			FlatMapFunction<String, Tuple2<String, String>> {

		private static final Pattern SEPARATOR = Pattern.compile("[\t,]");

		@Override
		public void flatMap(String s,
				Collector<Tuple2<String, String>> collector) throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				String domain = tokens[0];
				String company = tokens[1];
				collector.collect(new Tuple2<String, String>(domain, company));
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

	public static class NodeReader implements
			FlatMapFunction<String, Tuple2<String, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<String, Long>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				String node = tokens[0];
				long nodeIndex = Long.parseLong(tokens[1]);
				collector.collect(new Tuple2<String, Long>(node, nodeIndex));
			}
		}
	}
}
