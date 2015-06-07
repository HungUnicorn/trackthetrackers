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

package io.ssc.trackthetrackers.analysis.extraction.company;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

// Aggregate the arcs to company level
// Output:(Long, Long)
public class ArcCompanyLevel {

	private static String argPathToDomainCompany = "/home/sendoh/trackthetrackers/analysis/src/resources/company/domainCompanyMapping";
	private static String argPathToCompanyIndex = "/home/sendoh/datasets/companyIndex.tsv";
	private static String argPathToEmbedArcs = Config.get("analysis.results.path") + "filterDotComArc";
	private static String argPathToThirdPartyIndex = Config.get("analysis.results.path") + "thirdPartyIndex";

	private static String argPathOut = Config.get("analysis.results.path") + "arcCompanyLevel";

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> longArcs = ReaderUtils.readArcs(env, argPathToEmbedArcs);
		DataSet<Tuple2<String, Long>> thirdPartyIndex = ReaderUtils.readNameAndId(env, argPathToThirdPartyIndex);

		// Name of third party, id of first party
		DataSet<Tuple2<String, Long>> stringArcs = longArcs.join(thirdPartyIndex).where(0).equalTo(1).projectSecond(0).projectFirst(1);

		DataSet<Tuple2<String, String>> domainCompany = ReaderUtils.readDomainCompanyFullName(env, argPathToDomainCompany);
		DataSet<Tuple2<String, Long>> companyIndex = ReaderUtils.readNameWithComma(env, argPathToCompanyIndex);

		// Domain, company index
		DataSet<Tuple2<String, Long>> domainAndCompanyIndex = domainCompany.join(companyIndex).where(1).equalTo(0).projectFirst(0).projectSecond(1);

		// Get company of third party for arcs
		DataSet<Tuple2<String, Long>> companyArcs = stringArcs.joinWithTiny(domainAndCompanyIndex).where(0).equalTo(0).projectSecond(1)
				.projectFirst(1);
		
		companyArcs.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();

	}

	public static class CompanyArcMapper extends RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		HashMap<Long, Long> trackerCompanyMap = new HashMap<Long, Long>();

		@Override
		public void open(Configuration parameters) throws Exception {

			ArrayList<Tuple2<Long, Long>> pldIndexWithcompanyIndex = (ArrayList) getRuntimeContext().getBroadcastVariable("pldIndexWithcompanyIndex");

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
}
