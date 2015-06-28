package io.ssc.trackthetrackers.analysis;

public class CompanyParser {

	// The company name is hidden or the domain against whois policy
	public static boolean isCompany(String company) {
		if (company != null) {
			if (company.length() > 2 && !company.toLowerCase().contains("privacy") && !company.toLowerCase().contains("proxy")
					&& !company.toLowerCase().contains("whois") && !company.toLowerCase().contains("a happy dreamhost customer")
					&& !company.toLowerCase().contains("1&1 internet") && !company.toLowerCase().contains("godaddy")
					&& !company.toLowerCase().contains("dnstination")) {
				return true;
			}
		}
		return false;

	}

	// The company name is hidden
	public static boolean isCompanyStrict(String company) {
		if (company != null) {
			if (isCompany(company)) {
				if (isCoporation(company) || isInc(company) || isLtd(company) || isLLC(company) || isIncorporated(company)) {
					return true;
				}
			}
		}
		return false;

	}

	public static boolean isCoporation(String company) {
		if (company.toLowerCase().contains("corporation")) {
			return true;
		}
		return false;
	}

	public static boolean isInc(String company) {
		if (company.toLowerCase().contains("inc")) {
			return true;
		}
		return false;
	}

	public static boolean isLtd(String company) {
		if (company.toLowerCase().contains("ltd")) {
			return true;
		}
		return false;
	}

	public static boolean isLLC(String company) {
		if (company.toLowerCase().contains("llc")) {
			return true;
		}
		return false;
	}

	public static boolean isIncorporated(String company) {
		if (company.toLowerCase().contains("incorporated")) {
			return true;
		}
		return false;
	}

	public static String readCompanyCommaRemoved(String companyWithComma) {
		String companyCommaRemoved = companyWithComma.replaceAll(",", " ");
		return companyCommaRemoved;
	}

}
