
public class Point {
	private String table;
	private Object time;
	private String value;
	
	public Point(String tableNameIfVirtual, Object pkValue, String valueAsString) {
		this.table = tableNameIfVirtual;
		this.time = pkValue;
		this.value = valueAsString;
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
		return "Point[t=" + table + "," + time + "," + value+ "]";
	}
}
