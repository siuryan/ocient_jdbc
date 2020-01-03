package com.ocient.jdbc;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.ocient.jdbc.XGStatement;
import com.ocient.jdbc.proto.PlanProtocol.PlanMessage;

//all of these tests are based on the fact that we are getting empty result sets
public class JDBCDriverTest {

	static Properties propTest;
	static String urlTest;
	
	public static void main(final String args[]) {
		try {
			Class.forName("com.ocient.jdbc.JDBCDriver");
		}
		catch(final Exception e) {
			System.out.println("Driver Load Exception");
			e.printStackTrace();
		}
		
		propTest = new Properties();
		propTest.setProperty("user", "jason");
		propTest.setProperty("password", "pwd");
		propTest.setProperty("force", "true");
		urlTest = "jdbc:ocient://localhost:4050/Test";
		
		
		
		boolean success = testAll();
		
		return;
		
	}
	
	static boolean testAll() {
		System.out.println("Running All\n");
		boolean success = true;
		success = schema() && success;
		success = results() && success;
		success = resultsMeta() && success;
		success = explain() && success;
		success = prepared() && success;
		success = preparedBad() && success;
		success = staticPrimitiveMetadata() && success;
		success = typeInfo() && success;
		
		if(success) {
			System.out.println("\ntestAll: success");
		}
		else {
			System.out.println("\ntestAll: failure");
		}
		return success;
	};
	
	static boolean schema() {
		System.out.print("Running Schema\t\t\t\t");
		boolean success = true;
		try {
			Connection conn = DriverManager.getConnection(urlTest, propTest);
			String mySchema = "mySchema";
			conn.setSchema(mySchema);
			String theirSchema = conn.getSchema();
			success = (mySchema.equals(theirSchema)) && success;
			conn.close();
		}
		catch(final Exception e) {
			success = false;
			System.out.println("Schema Exception");
			e.printStackTrace();
		}
		if(success) {
			System.out.print("success\n");
		}
		else {
			System.out.print("failure\n");
		}
		return success;
	};
	
	static boolean prepared() {
		System.out.print("Running Prepared\t\t\t");
		boolean success = true;
		boolean whileCheck = true;
		try {
			//just don't want to throw an exception
			Connection conn = DriverManager.getConnection(urlTest, propTest);

			//bigDecimal Test
			PreparedStatement pstmt = conn.prepareStatement("select c1 from sys.dummy10000 where c1 = ?");
			pstmt.setBigDecimal(1, new BigDecimal(1));
			ResultSet rs = pstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
						
			whileCheck = rs.next();
			
			while(whileCheck)
			{
				try {
					BigDecimal tmp = rs.getBigDecimal("c1");
					whileCheck = rs.next();
				}
				catch (final Exception e) {
					success = false;
					whileCheck = false;
					System.out.println("BigDecimal test fail");
				}
			}
			rs.close();

			//bool test	
			pstmt = conn.prepareStatement("select temp from (select c1, BOOLEAN('false') as temp from sys.dummy10000) where temp = ?");
			pstmt.setBoolean(1, false);
			rs = pstmt.executeQuery();
			
			whileCheck = rs.next();
			
			while(whileCheck)
			{
				try {
					boolean tmp = rs.getBoolean("temp");
					whileCheck = rs.next();
				}
				catch (final Exception e) {
					success = false;
					whileCheck = false;
					System.out.println("setBoolean test failed");
				}
			}
			rs.close();
			
			//byte test
			byte theByte = 1;
			pstmt = conn.prepareStatement("select temp from (select c1, BYTE(c1) as temp from sys.dummy100) where temp = ?");
			pstmt.setByte(1, theByte);
			rs = pstmt.executeQuery();
			
			whileCheck = rs.next();
			
			while(whileCheck)
			{
				try {
					byte tmp = rs.getByte("temp");
					whileCheck = rs.next();
				}
				catch (final Exception e) {
					success = false;
					whileCheck = false;
					System.out.println("Byte test failed");
				}
			}			
			rs.close();
			
			//Date test
			pstmt = conn.prepareStatement("select temp from (select c1, DATE('1991-05-07') as temp from sys.dummy1000) where temp = ?");
			pstmt.setDate(1, new Date(0));
			rs = pstmt.executeQuery();
			
			whileCheck = rs.next();
			
			while(whileCheck)
			{
				try {
					Date tmp = rs.getDate("temp");
					whileCheck = rs.next();
				}
				catch (final Exception e)
				{
					success = false;
					whileCheck = false;
					System.out.println("Date test failed");
				}
			}
			rs.close();
			
			//Double Test
			pstmt = conn.prepareStatement("select temp from (select c1, DOUBLE(c1) as temp from sys.dummy1000) where temp = ?");
			pstmt.setDouble(1, 1.0);
			rs = pstmt.executeQuery();
			
			whileCheck = rs.next();
			
			while(whileCheck)
			{
				try {
					double tmp = rs.getDouble(1);
					whileCheck = rs.next();
				}
				catch (final Exception e)
				{
					success = false;
					whileCheck = false;
					System.out.println("Double check failed");
				}
			}
			rs.close();
			
			//Float test
			pstmt = conn.prepareStatement("select temp from (select c1, FLOAT(c1) as temp from sys.dummy1000) where temp = ?");
			pstmt.setFloat(1, new Float(4.0));
			rs = pstmt.executeQuery();
			
			whileCheck = rs.next();
			
			while(whileCheck)
			{
				try {
					float tmp = rs.getFloat(1);
					whileCheck = rs.next();
				}
				catch (final Exception e)
				{
					success = false;
					whileCheck = false;
					System.out.println("float check failed");
				}
			}
			rs.close();

			//Long Test
			long longTemp = 2147483648l;
			pstmt = conn.prepareStatement("select temp from (select c1, POWER(c1, 4) as temp from sys.dummy10000) where temp = ?");
			pstmt.setLong(1, longTemp); //2147483647 is the maximum int value, anything greater should be a long.
			rs = pstmt.executeQuery();
			
			whileCheck = rs.next();
			
			while(whileCheck) {
				try {
					long tmp = rs.getLong(1);
					whileCheck = rs.next();
				}
				catch (final Exception e) {
					success = false;
					whileCheck = false;
					System.out.println("Long Check failed");
				}
			}
			rs.close();
			
			//this doesnt actually work yet
			//pstmt = conn.prepareStatement("select temp from (select c1, POWER(c1, 4) as temp from sys.dummy10000) where temp = ?");
			pstmt.setNull(1, 4);
			//rs = pstmt.executeQuery();
						
			
			//Object test
			pstmt = conn.prepareStatement("select temp from (select c1, TIMESTAMP(BIGINT(c1)) as temp from sys.dummy20000) where temp = ?");
			pstmt.setObject(1, new Timestamp(0));
			rs = pstmt.executeQuery();
			
			whileCheck = rs.next();
			
			while(whileCheck) {
				try {
					Date tmp = rs.getDate(1);
					whileCheck = rs.next();
				}
				catch (final Exception e)
				{
					success = false;
					whileCheck = rs.next();
					System.out.println("Timestamp check failed");
				}
			}
			rs.close();
			
			//Short Test
			short theShort = 4;
			pstmt.clearParameters();
			pstmt = conn.prepareStatement("select temp from (select c1, SMALLINT(c1) as temp from sys.dummy32000) where temp = ?");
			pstmt.setShort(1, theShort);
			rs = pstmt.executeQuery();
			
			whileCheck = rs.next();
			
			while(whileCheck)
			{
				try {
					short tmp = rs.getShort(1);
					whileCheck = rs.next();
				}
				catch(final Exception e)
				{
					success = false;
					whileCheck = rs.next();
					System.out.println("short Check failed");
				}
			}
			rs.close();
			
			
			//string test
			pstmt = conn.prepareStatement("select temp from (select c1, 'hi' as temp from sys.dummy100) where temp = ?");
			pstmt.setString(1, "hi");
			rs = pstmt.executeQuery();
			
			whileCheck = rs.next();
			
			while(whileCheck)
			{
				try {
					String tmp = rs.getString(1);
					whileCheck = rs.next();
				}
				catch(final Exception e)
				{
					success = false;
					whileCheck = rs.next();
					System.out.println("String check failed");
				}
			}
			rs.close();
			
			//Timestamp test
			pstmt = conn.prepareStatement("select temp from (select c1, TIMESTAMP(BIGINT(c1)) as temp from sys.dummy20000) where temp = ?");
			pstmt.setTimestamp(1, new Timestamp(0));
			rs = pstmt.executeQuery();
			
			whileCheck = rs.next();
			
			while(whileCheck) {
				try {
					Date tmp = rs.getDate(1);
					whileCheck = rs.next();
				}
				catch (final Exception e)
				{
					success = false;
					whileCheck = rs.next();
					System.out.println("Timestamp check failed");
				}
			}
			rs.close();
		
			pstmt.clearParameters();
		
			pstmt = conn.prepareStatement("select c1 from sys.dummy100000 where c1 = ?");
			pstmt.setInt(1, 4);
			rs = pstmt.executeQuery();
			
			whileCheck = rs.next();
			
			while(whileCheck)
			{
				try {
					int tmp = rs.getInt(1);
					whileCheck = rs.next();
				}
				catch (final Exception e)
				{
					success = false;
					whileCheck = rs.next();
					System.out.println("int check failed");
				}
			}
			rs.close();
			conn.close();
		}
		catch(final Exception e) {
			success = false;
			System.out.println("Prepared Exception");
			e.printStackTrace();
		}

		if(success) {
			System.out.print("success\n");
		}
		else {
			System.out.print("failure\n");
		}
		return success;
	};
	
	static boolean preparedBad() {
		System.out.print("Running PreparedBad\t\t\t");
		boolean success = false;
		try {
			//just don't want to throw an exception
			Connection conn = DriverManager.getConnection(urlTest, propTest);
			PreparedStatement pstmt = conn.prepareStatement("select c1 from sys.dummy10000 where c1 = ?");
			pstmt.setString(1, "hi");
			ResultSet rs = pstmt.executeQuery();
			conn.close();
			System.out.println("PreparedBad No Exception");
		}
		catch(final Exception e) {
			success = true;
		}

		if(success) {
			System.out.print("success\n");
		}
		else {
			System.out.print("failure\n");
		}
		return success;
	};

	static boolean results() {
		System.out.print("Running Results\t\t\t\t");
		boolean success = true;
		try {
			Connection conn = DriverManager.getConnection(urlTest, propTest);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select * from sys.dummy1000 order by c1");
			success = rs.isBeforeFirst() && success;
			success = rs.next() && success;
			success = !rs.isAfterLast() && success;
			conn.close();
		}
		catch(final Exception e) {
			success = false;
			System.out.println("Results Exception");
			e.printStackTrace();
		}
		if(success) {
			System.out.print("success\n");
		}
		else {
			System.out.print("failure\n");
		}
		return success;
	};
	
	static boolean resultsMeta() {
		System.out.print("Running ResultsMeta\t\t\t");
		boolean success = true;
		try {
			Connection conn = DriverManager.getConnection(urlTest, propTest);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select * from (select c1, sin(c1) as sin, cos(c1), tan(c1) from sys.dummy16) order by sin");
			ResultSetMetaData rsmd = rs.getMetaData();
			success = (rsmd.getColumnCount() == 4) && success;
			//just want none of this to throw
			for(int i = 1; i <= 4; ++i) {
				Object temp1 = rsmd.getColumnName(i);
				Object temp2 = rsmd.getColumnClassName(i);
				Object temp3 = rsmd.getColumnDisplaySize(i);
				Object temp4 = rsmd.getColumnLabel(i);
				Object temp5 = rsmd.getColumnType(i);
				Object temp6 = rsmd.getColumnTypeName(i);
				
				if( (temp1 != null) && (temp2 != null) && (temp3 != null) && (temp4 != null) && (temp5 != null) && (temp6 != null))
				{
					success = true;
				}
				else {
					success = false;
				}
			}
			conn.close();
		}
		catch(final Exception e) {
			success = false;
			System.out.println("ResultsMeta Exception");
			e.printStackTrace();
		}
		if(success) {
			System.out.print("success\n");
		}
		else {
			System.out.print("failure\n");
		}
		return success;
	};
	
	static boolean explain() {
		System.out.print("Running Explain\t\t\t\t");
		boolean success = true;
		try {
			Connection conn = DriverManager.getConnection(urlTest, propTest);
			Statement stmt = conn.createStatement();
			PlanMessage plan = ((XGStatement)stmt).explain("select * from (select c1, sin(c1), char(c1) from sys.dummy10000) order by c1");
			success = (plan != com.ocient.jdbc.proto.PlanProtocol.PlanMessage.getDefaultInstance()) && success;
			conn.close();
		}
		catch(final Exception e) {
			success = false;
			System.out.println("Explain Exception");
			e.printStackTrace();
		}
		if(success) {
			System.out.print("success\n");
		}
		else {
			System.out.print("failure\n");
		}
		return success;
	};
	
	// these are labeled "static" because they do not depend on the data in the database,
	// but the version and the lists of functions supported are moving targets and should be kept up to date
	static boolean staticPrimitiveMetadata() {
		System.out.print("Running staticPrimitiveMetadata\t\t");
		boolean success = true;
		try {
			Connection conn = DriverManager.getConnection(urlTest, propTest);
			DatabaseMetaData dbmd = conn.getMetaData();
			String s = dbmd.getSQLKeywords();
			success = s.equals("OFFSET,LIMIT,MLMODEL,MULTIPLE,LINEAR,REGRESSION");
			s = dbmd.getNumericFunctions();
			success = s.equals("cos,sin,tan,acos,asin,atan,sinh,cosh,tanh,asinh,acosh,atanh,abs,ceil,floor,round,exp,ln,log2,log10,power,sqrt,mod,rand") && success;
			s = dbmd.getStringFunctions();
			success = s.equals("length,concat,lower,upper,ltrim,rtrim,trim,substring") && success;
			s = dbmd.getTimeDateFunctions();
			success = s.equals("day,days,month,months,year,years,hour,hours,minute,minutes,second,seconds,millisecond,milliseconds,current_date,current_timestamp") && success;			
			s = dbmd.getSystemFunctions();
			success = s.equals("") && success;
			int i = dbmd.getDatabaseMajorVersion();
			success = (i == 1) && success;
			i = dbmd.getDatabaseMinorVersion();
			success = (i == 0) && success;
		}
		catch(final Exception e) {
			success = false;
			System.out.println("staticPrimitiveMetadata Exception");
			e.printStackTrace();
		}
		if(success) {
			System.out.print("success\n");
		}
		else {
			System.out.print("failure\n");
		}
		return success;
	}
	
	// all the types are there
	// label and index row accessors match
	// we can access all of the columns
	// column types are accurate and consistent
	static boolean typeInfo() {
		System.out.print("Running getTypeInfo\t\t\t");
		boolean success = true;
		try {
			Connection conn = DriverManager.getConnection(urlTest, propTest);
			DatabaseMetaData dbmd = conn.getMetaData();
			ResultSet rs = dbmd.getTypeInfo();
			ResultSetMetaData rsmd = rs.getMetaData();
			Set<String> types = Stream.of("BOOLEAN","INT8","UINT8","INT16","UINT16","INT32","UINT32","INT64","UINT64","FLOAT32",
					"FLOAT64","CHAR","VARCHAR","BLOB","DATE","TIMESTAMP","IPV4","UUID","NUMERICXY").collect(Collectors.toSet());
			while (rs.next()) {
				String name = rs.getString("TYPE_NAME");
				success = name.equals(rs.getString(1)) && success;
				types.remove(name);
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					int type = rsmd.getColumnType(i);
					// other type accessors should be tested as well, but the typeInfo table covers only these
					switch (type) {
	                    case java.sql.Types.VARCHAR:
	                    	rs.getString(i);
	                    	break;
	                    case java.sql.Types.SMALLINT:
	                    	rs.getShort(i);
	                    	break;
	                    case java.sql.Types.INTEGER:
	                    	rs.getInt(i);
	                    	break;
	                    case java.sql.Types.BOOLEAN:
	                    	rs.getBoolean(i);
	                    	break;
	                    default:
	                    	
                    }
				}
			}
			if (!types.isEmpty()) {
				System.out.println("Did not find all types");
				success = false;
			}
		}
		catch(final Exception e) {
			success = false;
			System.out.println("getTypeInfo Exception");
			e.printStackTrace();
		}
		if(success) {
			System.out.print("success\n");
		}
		else {
			System.out.print("failure\n");
		}
		return success;
	}
	
};
