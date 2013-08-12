package fromporm.conv;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;


public class Converters {

	public static final StringConverter STRING_CONVERTER = new StringConverter();
	public static final BooleanConverter BOOLEAN_CONVERTER = new BooleanConverter();
	public static final ShortConverter SHORT_CONVERTER = new ShortConverter();
	public static final IntConverter INT_CONVERTER = new IntConverter();
	public static final LongConverter LONG_CONVERTER = new LongConverter();
	public static final FloatConverter FLOAT_CONVERTER = new FloatConverter();
	public static final DoubleConverter DOUBLE_CONVERTER = new DoubleConverter();
	public static final ByteConverter BYTE_CONVERTER = new ByteConverter();
	public static final ByteArrayConverter BYTE_ARRAY_CONVERTER = new ByteArrayConverter();
	public static final BaseConverter BIGINTEGER_CONVERTER = new BigIntegerConverter();
	public static final BaseConverter BIGDECIMAL_CONVERTER = new BigDecimalConverter();


	private static byte[] intToBytes(int val) {
		try {
			ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(outBytes);
			out.writeInt(val);
			byte[] outData = outBytes.toByteArray();
			return outData;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static class BigDecimalConverter extends BaseConverter {
		@Override
		public byte[] convertToNoSqlImpl(Object input) {
			BigDecimal value = (BigDecimal) input;
			BigInteger bi = value.unscaledValue();
			Integer scale = value.scale();
			byte[] bibytes = bi.toByteArray();
			byte[] sbytes = intToBytes(scale);
			byte[] bytes = new byte[bi.toByteArray().length+4];
			
			for (int i = 0 ; i < 4 ; i++) bytes[i] = sbytes[i];
			for (int i = 4 ; i < bibytes.length+4 ; i++) bytes[i] = bibytes[i-4];

			return bytes;
		}

		@Override
		public Object convertFromNoSqlImpl(byte[] value) {
			ByteBuffer buf = ByteBuffer.wrap(value);
			int scale = buf.getInt();
			byte[] bibytes = new byte[buf.remaining()];
			buf.get(bibytes);
			 
			BigInteger bi = new BigInteger(bibytes);
			return new BigDecimal(bi,scale);
		}

		@Override
		public Object convertToType(String value) {
			return new BigDecimal(value);
		}
	}
	
	public static class BigIntegerConverter extends BaseConverter {
		@Override
		public byte[] convertToNoSqlImpl(Object value) {
			BigInteger val = (BigInteger)value;
			return val.toByteArray();
		}

		@Override
		public Object convertFromNoSqlImpl(byte[] value) {
			return new BigInteger(value);
		}

		@Override
		public Object convertToType(String value) {
			return new BigInteger(value);
		}
		
	}
	
	public static class ByteArrayConverter extends BaseConverter {

		@Override
		public byte[] convertToNoSqlImpl(Object value) {
			return (byte[])value;
		}

		@Override
		public Object convertFromNoSqlImpl(byte[] value) {
			return value;
		}
		
		@Override
		public String convertToString(Object dbValue) {
			byte[] data = (byte[]) dbValue;
			return new String(Hex.encodeHex(data));
		}

		@Override
		public Object convertToType(String value) {
			try {
				return Hex.decodeHex(value.toCharArray());
			} catch (DecoderException e) {
				throw new RuntimeException("could not decode", e);
			}
		}
	}
	
	public static abstract class BaseConverter implements Converter {
		
		public final byte[] convertToNoSql(Object value) {
			if(value == null)
				return null;
			return convertToNoSqlImpl(value);
		}

		public final Object convertFromNoSql(byte[] value) {
			if(value == null)
				return null;
			return convertFromNoSqlImpl(value);
		}

		public final Object convertStringToType(String value) {
			if(value == null)
				return null;
			return convertToType(value);
		}

		public final String convertTypeToString(Object value) {
			if(value == null)
				return null;
			return convertToString(value);
		}
		
		protected abstract Object convertToType(String value);

		protected abstract byte[] convertToNoSqlImpl(Object value);
		protected abstract Object convertFromNoSqlImpl(byte[] value);
		
		//This is overloaded for stuff like byte[] types which need to convert to Hex
		//or for LocalDateTime as well to use correct formatter.
		protected String convertToString(Object value) {
			return value+"";
		}
	}
	
	public static abstract class DecimalConverter extends BaseConverter {
		public byte[] convertToNoSqlImpl(Object value) {
			BigDecimal dec = convertToForSmallStorage(value);
			return BIGDECIMAL_CONVERTER.convertToNoSql(dec);
		}
		public Object convertFromNoSqlImpl(byte[] data) {
			BigDecimal bigD = (BigDecimal) BIGDECIMAL_CONVERTER.convertFromNoSql(data);
			return convertFromForSmallStorage(bigD);
		}
		protected abstract Object convertFromForSmallStorage(BigDecimal bigD);
		protected abstract BigDecimal convertToForSmallStorage(Object value);
	}

	public static abstract class IntegerConverter extends BaseConverter {
		public byte[] convertToNoSqlImpl(Object value) {
			BigInteger dec = convertToForSmallStorage(value);
			return BIGINTEGER_CONVERTER.convertToNoSql(dec);
		}
		public Object convertFromNoSqlImpl(byte[] data) {
			BigInteger bigD = (BigInteger) BIGINTEGER_CONVERTER.convertFromNoSql(data);
			return convertFromForSmallStorage(bigD);
		}
		protected abstract Object convertFromForSmallStorage(BigInteger bigD);
		protected abstract BigInteger convertToForSmallStorage(Object value);
	}
	
	public static class StringConverter extends BaseConverter {
		
		@Override
		public byte[] convertToNoSqlImpl(Object value) {
			try {
				String temp = (String) value;
				return temp.getBytes("UTF8");
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		@Override
		public Object convertFromNoSqlImpl(byte[] bytes) {
			try {
				return new String(bytes, "UTF8");
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		protected Object convertToType(String value) {
			return value;
		}
	}
	
	public static class ByteConverter extends IntegerConverter {
		@Override
		protected Object convertToType(String value) {
			return Byte.parseByte(value);
		}
		@Override
		protected Object convertFromForSmallStorage(BigInteger bigD) {
			return bigD.byteValue();
		}

		@Override
		protected BigInteger convertToForSmallStorage(Object value) {
			Byte b = (Byte)value;
			return BigInteger.valueOf(b.byteValue());
		}
	}
	
	public static class ShortConverter extends IntegerConverter {
		@Override
		protected Object convertToType(String value) {
			return Short.parseShort(value);
		}
		@Override
		protected Object convertFromForSmallStorage(BigInteger bigD) {
			return bigD.shortValue();
		}
		@Override
		protected BigInteger convertToForSmallStorage(Object value) {
			Short s = (Short)value;
			return BigInteger.valueOf(s);
		}
	}
	
	public static class IntConverter extends IntegerConverter {
		@Override
		protected Object convertToType(String value) {
			return Integer.parseInt(value);
		}
		@Override
		protected Object convertFromForSmallStorage(BigInteger bigD) {
			return bigD.intValue();
		}
		@Override
		protected BigInteger convertToForSmallStorage(Object value) {
			Integer val = (Integer)value;
			return BigInteger.valueOf(val.intValue());
		}
	}

	public static class LongConverter extends IntegerConverter {
		@Override
		protected Object convertToType(String value) {
			return Long.parseLong(value);
		}
		@Override
		protected Object convertFromForSmallStorage(BigInteger bigD) {
			return bigD.longValue();
		}
		@Override
		protected BigInteger convertToForSmallStorage(Object value) {
			Long val = (Long)value;
			return BigInteger.valueOf(val);
		}
/*		This is issue #83
        @Override
		public byte[] convertToNoSqlImpl(Object value) {
			// Cassandra expects an 8-byte Long value, but some versions
			// of Java produce fewer bytes (eg 6 bytes in Java 1.7.0)
			// We need to detect when this happens extend the array size
			// We cannot use Arrays.copyOf(byte[], int newLength) because
			// this right-pads the new array, but we need a left-padded
			// array due to being big-endian.

			// How many bytes does Cassandra expect?
			int targetLength = 8;

			byte[] b1 = super.convertToNoSqlImpl(value);

			// Exit early if we already have an array of the right length
			if (b1.length == targetLength) return b1;

			// We need to copy the bytes offset by the delta length
			byte[] b2 = new byte[targetLength];

			// Left pad the array
			int deltaLength = targetLength - b1.length;
			for (int y = 0; y < b1.length; y++) {
				b2[y + deltaLength] = b1[y];
			}

			return b2;
		}*/
	}
	
	public static class FloatConverter extends DecimalConverter {
		@Override
		protected Object convertToType(String value) {
			return Float.parseFloat(value);
		}

		@Override
		protected Object convertFromForSmallStorage(BigDecimal bigD) {
			return bigD.floatValue();
		}

		@Override
		protected BigDecimal convertToForSmallStorage(Object value) {
			Float f = (Float)value;
			//NOTE: Must convert to string here or when you feed BigDecimal, the
			//2.33 that you had given the float becomes 2.329999999991234... 
			return new BigDecimal(f.floatValue()+"");
		}
	}
	
	public static class DoubleConverter extends DecimalConverter {
		@Override
		protected Object convertToType(String value) {
			return Double.parseDouble(value);
		}

		@Override
		protected Object convertFromForSmallStorage(BigDecimal bigD) {
			return bigD.doubleValue();
		}

		@Override
		protected BigDecimal convertToForSmallStorage(Object value) {
			Double d = (Double)value;
			//NOTE: Must convert to string here or when you feed BigDecimal, the
			//2.33 that you had given the float becomes 2.329999999991234...
			return new BigDecimal(""+d.doubleValue());
		}
	}
	
	public static class BooleanConverter extends BaseConverter {
		@Override
		public byte[] convertToNoSqlImpl(Object value) {
			Boolean b = (Boolean) value;
			if(b)
				return StandardConverters.convertToBytes(1);
			return StandardConverters.convertToBytes(0);
		}
		
		@Override
		public Object convertFromNoSqlImpl(byte[] bytes) {
			Integer value = StandardConverters.convertFromBytes(Integer.class, bytes);
			if(value == 1)
				return true;
			return false;
		}
		
		@Override
		protected Object convertToType(String value) {
			return Boolean.parseBoolean(value);
		}
	}
}
