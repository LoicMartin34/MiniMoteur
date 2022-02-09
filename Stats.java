import java.util.HashMap;

public class Stats{
	
	public HashMap<String, long[]> allTimeMeasures;
	public HashMap<String, String> allMeasures;
	
	public Stats() {
		allTimeMeasures = new HashMap<String, long[]>();
		allMeasures = new HashMap<String, String>();
	}
	
	public void startTimeMeasure(String timeKey) {
		allTimeMeasures.put(timeKey, new long[2]);
		allTimeMeasures.get(timeKey)[0] = System.currentTimeMillis();
	}
	
	public void pauseTimeMeasure(String timeKey) {
		allTimeMeasures.get(timeKey)[1] = System.currentTimeMillis();
	}
	
	public long getTimeMeasure(String timeKey) {
		return allTimeMeasures.get(timeKey)[1] - allTimeMeasures.get(timeKey)[0];
	}
	
	public void saveMeasure(String measureKey, String measure) {
		allMeasures.put(measureKey, measure);
	}
	
	public String getMeasure(String measureKey) {
		return allMeasures.get(measureKey);
	}
	

}
