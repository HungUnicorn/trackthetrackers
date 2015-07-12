package io.ssc.trackthetrackers.analysis.privacyhazard;


/* Give the privacy hazard index to each category 
 */
public class HazardIndex {	

	Category category;
	
	public enum Category {
		Adult(1), Arts(1), Business(1), Computers(1), Games(1), Health(5), Home(1), Kids_and_Teens(1),
		News(1), Recreation(3), Reference(1), Science(1), Shopping(1), Society(1), Sports(1); 
		
		private int hazard;
		
		private Category(int hazard) {
			this.hazard = hazard;
		}
	}
	
	public int getValue(Category category){
		return category.hazard;
		
	}
	
}
