package io.ssc.trackthetrackers.analysis.userintent;

import io.ssc.trackthetrackers.Config;
import io.ssc.trackthetrackers.analysis.ReaderUtils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;

/* Get the user intent distribution
 * Embed in the same category is viewed as one instance i.e. Third party can see the information of this category 
 * Thus, duplicate category of the same third party is removed and only one (category,third party) is remained 
 * e.g. (Google, sports) (Google, games) (Google, sports) become (Google, sports) (Google, games) and duplicate sports is removed
 * 
 * */
public class UserIntent {

	private static String argPathSiteCategory = Config.get("analysis.results.path") + "/UserIntent/" + "allCategorySites";
	private static String argPathToPLD = Config.get("webdatacommons.pldfile.unzipped");
	private static String argPathToEmbedArcs = Config.get("analysis.results.path") + "distinctArcCompanyLevel";
	private static String argPathToCategoryValue = Config.get("analysis.results.path") + "/UserIntent/" + "categoryValue.csv";

	private static String argPathIntent = Config.get("analysis.results.path") + "/UserIntent/" + "userIntent";
	private static String argPathEmbedCategory = Config.get("analysis.results.path") + "/UserIntent/" + "embedCategory";

	public static void main(String args[]) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> embedArcs = ReaderUtils.readArcs(env, argPathToEmbedArcs);

		DataSet<Tuple2<String, Double>> categoryValue = ReaderUtils.readStringAndValue(env, argPathToCategoryValue);

		DataSet<Tuple2<String, String>> domainCategory = ReaderUtils.readDomainCategory(env, argPathSiteCategory);

		DataSet<Tuple2<String, Long>> pldNodes = ReaderUtils.readNameAndId(env, argPathToPLD);

		// Get category and first party ID
		DataSet<Tuple2<String, Long>> categoryPldID = domainCategory.joinWithHuge(pldNodes).where(0).equalTo(0).projectFirst(1).projectSecond(1);

		// Get third party ID and category
		DataSet<Tuple2<Long, String>> embedCategory = categoryPldID.join(embedArcs).where(1).equalTo(1).projectSecond(0).projectFirst(0);

		// Duplicate category of the same third party is removed
		DataSet<Tuple2<Long, String>> distinctEmbedCategory = embedCategory.distinct();

		embedCategory.writeAsCsv(argPathEmbedCategory, WriteMode.OVERWRITE);

		// Get (Company, 1st party, value of 1st party)
		DataSet<Tuple2<Long, Double>> arcsValues = distinctEmbedCategory.joinWithTiny(categoryValue).where(1).equalTo(0).projectFirst(0)
				.projectSecond(1);

		DataSet<Tuple2<Long, Double>> companyIDAndValue = arcsValues.groupBy(0).aggregate(Aggregations.SUM, 1);

		companyIDAndValue.writeAsCsv(argPathIntent, WriteMode.OVERWRITE);

		env.execute();

	}
}
