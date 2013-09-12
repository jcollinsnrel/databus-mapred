
public class Point {
	private String table;
	private Object time;
	private String value;
	private Object partKey;
	private long time2;

	public Point(String tableNameIfVirtual, Object pkValue, long time, String valueAsString, Object partKey) {
		this.table = tableNameIfVirtual;
		this.time = pkValue;
		this.time2 = time;
		this.value = valueAsString;
		this.partKey = partKey;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public Object getTime() {
		return time;
	}

	public void setTime(Object time) {
		this.time = time;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "Point[t=" + table + "," + time + "," +time2+","+ value+ ",p="+partKey+"]";
	}
}
