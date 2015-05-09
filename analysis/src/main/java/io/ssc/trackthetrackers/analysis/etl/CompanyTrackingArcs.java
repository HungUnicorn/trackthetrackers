/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Hung Chang
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.ssc.trackthetrackers.analysis.etl;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.etl.OneModeProjection.ArcReader;
import io.ssc.trackthetrackers.analysis.undirected.TopDegree.NodeReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Aggregate the arcs to company level
// (Long)
public class CompanyTrackingArcs {

	private static String argPathToDomainCompany = "/home/sendoh/trackthetrackers/analysis/src/resources/company/DomainAndCompany.csv";
	private static String argPathToCompanyIndex = "/home/sendoh/datasets/companyWithIndex";
	private static String argPathToTrackingArcs = Config
			.get("analysis.trackingraphsample.path");
	private static String argPathToPLD = Config
			.get("webdatacommons.pldfile.unzipped");
	private static String argPathOut = Config.get("analysis.results.path")
			+ "TrackingArcsCompanyLevel";

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputTrackingArcs = env
				.readTextFile(argPathToTrackingArcs);
		DataSet<Tuple2<Long, Long>> trackingArcs = inputTrackingArcs
				.flatMap(new ArcReader());

		DataSource<String> inputDomainCompany = env
				.readTextFile(argPathToDomainCompany);
		DataSet<Tuple2<String, String>> domainCompany = inputDomainCompany
				.flatMap(new DomainReader());

		DataSource<String> inputNodePLD = env.readTextFile(argPathToPLD);
		DataSet<Tuple2<String, Long>> pldNodes = inputNodePLD
				.flatMap(new IndexReader());

		DataSource<String> inputCompanyIndex = env
				.readTextFile(argPathToCompanyIndex);
		DataSet<Tuple2<String, Long>> companyIndex = inputCompanyIndex
				.flatMap(new CompanyIndexReader());

		// Get pldIndex and company
		DataSet<Tuple2<Long, String>> pldIndexWithcompany = domainCompany
				.join(pldNodes).where(0).equalTo(0)
				.flatMap(new ProjectCompanyPLD());

		// Get pldIndex and company index
		DataSet<Tuple2<Long, Long>> pldIndexWithcompanyIndex = pldIndexWithcompany
				.join(companyIndex).where(1).equalTo(0)
				.flatMap(new ProjectIndexOfDomainAndCompany());

		// Get company of third party for arcs
		DataSet<Tuple2<Long, Long>> companyArcs = trackingArcs.map(
				new CompanyArcMapper()).withBroadcastSet(
				pldIndexWithcompanyIndex, "pldIndexWithcompanyIndex");

		companyArcs.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

	}

	public static class ProjectIndexOfDomainAndCompany
			implements
			FlatMapFunction<Tuple2<Tuple2<Long, String>, Tuple2<String, Long>>, Tuple2<Long, Long>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, String>, Tuple2<String, Long>> value,
				Collector<Tuple2<Long, Long>> collector) throws Exception {
			Long pldIndex = value.f0.f0;
			Long companyIndex = value.f1.f1;
			collector.collect(new Tuple2<Long, Long>(pldIndex, companyIndex));
		}
	}

	public static class CompanyArcMapper extends
			RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		HashMap<Long, Long> trackerCompanyMap = new HashMap<Long, Long>();

		@Override
		public void open(Configuration parameters) throws Exception {

			ArrayList<Tuple2<Long, Long>> pldIndexWithcompanyIndex = (ArrayList) getRuntimeContext()
					.getBroadcastVariable("pldIndexWithcompanyIndex");

			for (Tuple2<Long, Long> index : pldIndexWithcompanyIndex) {
				Long id = index.f0;
				Long company = index.f1;
				trackerCompanyMap.put(id, company);
			}
		}

		@Override
		public Tuple2<Long, Long> map(Tuple2<Long, Long> arc) throws Exception {
			Long tracker = arc.f0;
			Long company = trackerCompanyMap.get(tracker);
			Long source, target;

			if (company != null) {
				source = company;
				target = arc.f1;
			}
			// Give 0l if it's not recognized as a company
			else {
				source = 0l;
				target = arc.f1;
			}
			return new Tuple2<Long, Long>(source, target);
		}
	}

	public static class IndexReader implements
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

	// Keep domains' company full name
	public static class DomainFullNameReader implements
			FlatMapFunction<String, Tuple2<String, String>> {

		@Override
		public void flatMap(String input,
				Collector<Tuple2<String, String>> collector) throws Exception {
			if (!input.startsWith("%")) {
				String domain = input.substring(0, input.indexOf(","));
				String company = input.substring(input.indexOf(",") + 1).trim();
				collector.collect(new Tuple2<String, String>(domain, company));
			}
		}
	}

	// Company index reader
	public static class CompanyIndexReader implements
			FlatMapFunction<String, Tuple2<String, Long>> {

		@Override
		public void flatMap(String input,
				Collector<Tuple2<String, Long>> collector) throws Exception {
			if (!input.startsWith("%")) {			
				String company = input.substring(0, input.indexOf(",")).trim();
				Long companyIndex = Long.parseLong(input.substring(input.indexOf(",") + 1));
				collector.collect(new Tuple2<String, Long>(company, companyIndex));
			}
		}
	}

	// Company's symbol only
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

	public static class ProjectCompanyPLD
			implements
			FlatMapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, Long>>, Tuple2<Long, String>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<String, String>, Tuple2<String, Long>> value,
				Collector<Tuple2<Long, String>> collector) throws Exception {
			Long pldIndex = value.f1.f1;
			String company = value.f0.f1;
			collector.collect(new Tuple2<Long, String>(pldIndex, company));
		}
	}
}
