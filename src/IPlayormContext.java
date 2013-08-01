import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;


public interface IPlayormContext {

    public void initialize(Object keyspace, Object cluster1, Object seeds1, Object port1, Object keyspace2, Object cluster2, Object seeds2, Object port2);
    public String getTableNameFromKey(byte[] key);
    public boolean sourceTableIsStream(String tableNameIfVirtual, byte[] key);
    public void postTimeSeries(String tableNameIfVirtual, Object pkValue, Object value, NoSqlTypedSession typedSession);
    public Object convertToStorage(DboColumnMeta col, Object someVal);
    public String getSourceIdColumnValue(String tableNameIfVirtual, byte[] key);
	public String getSourceIdColumnName(String tableNameIfVirtual);
	public Number sourceConvertFromBytes(String tableNameIfVirtual, String columnName, byte[] valuearray);
	public String bytesToString(byte[] namearray);
}
