import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;


public interface IPlayormContext {

    public void initialize(String keyspace, String cluster1, String seeds1, String port1, String keyspace2, String cluster2, String seeds2, String port2);
    public String getTableNameFromKey(byte[] key);
    public boolean sourceTableIsStream(String tableNameIfVirtual, byte[] key);
    public void postTimeSeries(String tableNameIfVirtual, Object pkValue, Object value, NoSqlTypedSession typedSession);
    public Object convertToStorage(DboColumnMeta col, Object someVal);
    public String getSourceIdColumnValue(String tableNameIfVirtual, byte[] key);
	public String getSourceIdColumnName(String tableNameIfVirtual);
	public Object sourceConvertFromBytes(String tableNameIfVirtual, String columnName, byte[] valuearray);
	public String bytesToString(byte[] namearray);
}
