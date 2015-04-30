package io.ssc.trackthetrackers.analysis.statistics;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.statistics.AcquisitionEffect.ArcReader;

import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Get the trackers ability and the publishers tracked in a category of domains
public class TrackInOneCategory {

	private static String argPathToDomainCategory = "/home/sendoh/datasets/Category/health.csv";
	private static String argPathToPLD = Config
			.get("webdatacommons.pldfile.unzipped");
	private static String argPathToTrackingArcs = Config
			.get("analysis.trackingraphsample.path");

	private static String argPathPublisherTracked = Config
			.get("analysis.results.path") + "Category/" + "PublisherTracked";

	private static String argPathTrackerTracked = Config
			.get("analysis.results.path") + "Category/" + "TrackerTracked";

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputTrackingArcs = env
				.readTextFile(argPathToTrackingArcs);

		DataSource<String> inputNodePLD = env.readTextFile(argPathToPLD);

		DataSource<String> inputDomainCategory = env
				.readTextFile(argPathToDomainCategory);

		DataSet<Tuple2<Long, Long>> trackingArcs = inputTrackingArcs
				.flatMap(new ArcReader());

		DataSet<Tuple2<String, Long>> pldNodes = inputNodePLD
				.flatMap(new NodeReader());

		DataSet<Tuple2<String, String>> domainCategory = inputDomainCategory
				.flatMap(new DomainCategoryReader());

		DataSet<Tuple1<Long>> PLDinCategory = domainCategory.join(pldNodes)
				.where(0).equalTo(0).flatMap(new ProjectCategoryID());

		DataSet<Tuple2<Long, Long>> ArcsInCategory = trackingArcs
				.join(PLDinCategory).where(1).equalTo(0)
				.flatMap(new ProjectCategoryArc());

		// (1, Tracker, Domain)
		DataSet<Tuple3<Long, Long, Long>> ArcsCountMapper = ArcsInCategory
				.flatMap(new SumMapper());

		DataSet<Tuple2<Long, Long>> TrackerWithNumDomainsTracked = ArcsCountMapper
				.groupBy(1).aggregate(Aggregations.SUM, 0)
				.<Tuple2<Long, Long>> project(1, 0);		

		DataSet<Tuple2<String, Long>> TrackerNameWithNumDomainsTracked = TrackerWithNumDomainsTracked
				.join(pldNodes).where(0).equalTo(1)
				.flatMap(new ProjectPLDName());

		DataSet<Tuple2<Long, Long>> DomainWithNumTracker = ArcsCountMapper
				.groupBy(2).aggregate(Aggregations.SUM, 0)
				.<Tuple2<Long, Long>> project(2, 0);		
		
		// Join with PLD Name
		DataSet<Tuple2<String, Long>> DomainNameWithNumTracker = DomainWithNumTracker
				.join(pldNodes).where(0).equalTo(1)
				.flatMap(new ProjectPLDName());
		
		TrackerNameWithNumDomainsTracked.writeAsText(argPathTrackerTracked,
				WriteMode.OVERWRITE);
		
		DomainNameWithNumTracker.writeAsText(argPathPublisherTracked,
				WriteMode.OVERWRITE);

		env.execute();
	}

	public static class ProjectPLDName
			implements
			FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<String, Long>>, Tuple2<String, Long>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Long>, Tuple2<String, Long>> value,
				Collector<Tuple2<String, Long>> collector) throws Exception {
			String tracker = value.f1.f0;
			Long count = value.f0.f1;
			collector.collect(new Tuple2<String, Long>(tracker, count));
		}
	}

	public static class SumMapper implements
			FlatMapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>> {

		@Override
		public void flatMap(Tuple2<Long, Long> arc,
				Collector<Tuple3<Long, Long, Long>> collector) throws Exception {
			Long source = arc.f0;
			Long target = arc.f1;

			collector.collect(new Tuple3<Long, Long, Long>(1L, source, target));
		}

	}

	public static class ProjectCategoryArc
			implements
			FlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple1<Long>>, Tuple2<Long, Long>> {

		@Override
		public void flatMap(Tuple2<Tuple2<Long, Long>, Tuple1<Long>> joinTuple,
				Collector<Tuple2<Long, Long>> collector) throws Exception {
			Long source = joinTuple.f0.f0;
			Long target = joinTuple.f0.f1;

			collector.collect(new Tuple2<Long, Long>(source, target));
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

	public static class ProjectCategoryID
			implements
			FlatMapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>, Tuple1<Long>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<String, String>, Tuple2<String, Long>> joinTuple,
				Collector<Tuple1<Long>> collector) throws Exception {

			Long ID = joinTuple.f1.f1;

			collector.collect(new Tuple1<Long>(ID));
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

	public static class DomainCategoryReader implements
			FlatMapFunction<String, Tuple2<String, String>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s,
				Collector<Tuple2<String, String>> collector) throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);
				String domain = tokens[0];
				String category = tokens[1];
				collector.collect(new Tuple2<String, String>(domain, category));
			}
		}
	}
}
