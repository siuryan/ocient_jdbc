package com.ocient.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ocient.jdbc.proto.ClientWireProtocol;

public class XGDatabaseMetaData implements DatabaseMetaData
{
	private static final Logger LOGGER = Logger.getLogger( "com.ocient.jdbc" );
	private final Connection conn;

	public XGDatabaseMetaData(final Connection conn)
	{
		this.conn = conn;
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		LOGGER.log(Level.INFO, "Called allProceduresAreCallable()");
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		LOGGER.log(Level.INFO, "Called allTablesAreSelectable()");
		return true;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		LOGGER.log(Level.INFO, "Called autoCommitFailureClosesAllResultSets()");
		return true;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		LOGGER.log(Level.INFO, "Called dataDefinitionCausesTransactionCommit()");
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		LOGGER.log(Level.INFO, "Called dataDefinitionIgnoredInTransactions()");
		return false;
	}

	@Override
	public boolean deletesAreDetected(final int type) throws SQLException {
		LOGGER.log(Level.INFO, "Called deletesAreDetected()");
		return false;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		LOGGER.log(Level.INFO, "Called doesMaxRowSizeIncludeBlobs()");
		return true;
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		LOGGER.log(Level.INFO, "Called generatedKeyAlwaysReturned()");
		return false;
	}

	@Override
	public ResultSet getAttributes(final String catalog, final String schemaPattern, final String typeNamePattern,
			final String attributeNamePattern) throws SQLException {
		LOGGER.log(Level.INFO, "Called getAttributes()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public ResultSet getBestRowIdentifier(final String catalog, final String schema, final String table,
			final int scope, final boolean nullable) throws SQLException {
		LOGGER.log(Level.INFO, "Called getBestRowIdentifier()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		LOGGER.log(Level.INFO, "Called getCatalogs()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		LOGGER.log(Level.INFO, "Called getCatalogSeparator()");
		return ".";
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		LOGGER.log(Level.INFO, "Called getCatalogTerm()");
		return "system";
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		LOGGER.log(Level.INFO, "Called getClientInfoProperties()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public ResultSet getColumnPrivileges(final String catalog, final String schema, final String table,
			final String columnNamePattern) throws SQLException {
		LOGGER.log(Level.INFO, "Called getColumnPrivileges()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
			final String columnNamePattern) throws SQLException {
		LOGGER.log(Level.INFO, "Called getColumns()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_COLUMNS, schemaPattern, tableNamePattern,
				columnNamePattern, true);
	}

	@Override
	public Connection getConnection() throws SQLException {
		LOGGER.log(Level.INFO, "Called getConnection()");
		return conn;
	}

	@Override
	public ResultSet getCrossReference(final String parentCatalog, final String parentSchema, final String parentTable,
			final String foreignCatalog, final String foreignSchema, final String foreignTable) throws SQLException {
		LOGGER.log(Level.INFO, "Called getCrossReference()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		LOGGER.log(Level.INFO, "Called getDatabaseMajorVersion()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataInt(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_DATABASE_MAJOR_VERSION);
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		LOGGER.log(Level.INFO, "Called getDatabaseMinorVersion()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataInt(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_DATABASE_MINOR_VERSION);
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		LOGGER.log(Level.INFO, "Called getDatabaseProductName()");
		return "Ocient";
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		LOGGER.log(Level.INFO, "Called getDatabaseProductVersion()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataString(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_DATABASE_PRODUCT_VERSION);
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		LOGGER.log(Level.INFO, "Called getDefaultTransactionIsolation()");
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public int getDriverMajorVersion() {
		LOGGER.log(Level.INFO, "Called getDriverMajorVersion()");
		return ((XGConnection) conn).getMajorVersion();
	}

	@Override
	public int getDriverMinorVersion() {
		LOGGER.log(Level.INFO, "Called getDriverMinorVersion()");
		return ((XGConnection) conn).getMinorVersion();
	}

	@Override
	public String getDriverName() throws SQLException {
		LOGGER.log(Level.INFO, "Called getDriverName()");
		return "Ocient JDBC Driver";
	}

	@Override
	public String getDriverVersion() throws SQLException {
		LOGGER.log(Level.INFO, "Called getDriverVersion()");
		return ((XGConnection) conn).getVersion();
	}

	@Override
	public ResultSet getExportedKeys(final String catalog, final String schema, final String table)
			throws SQLException {
		LOGGER.log(Level.INFO, "Called getExportedKeys()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		LOGGER.log(Level.INFO, "Called getExtraNameCharacters()");
		return "";
	}

	@Override
	public ResultSet getFunctionColumns(final String catalog, final String schemaPattern,
			final String functionNamePattern, final String columnNamePattern) throws SQLException {
		LOGGER.log(Level.INFO, "Called getFunctionColumns()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public ResultSet getFunctions(final String catalog, final String schemaPattern, final String functionNamePattern)
			throws SQLException {
		LOGGER.log(Level.INFO, "Called getFunctions()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		LOGGER.log(Level.INFO, "Called getIdentifierQuoteString()");
		return "\"";
	}

	@Override
	public ResultSet getImportedKeys(final String catalog, final String schema, final String table)
			throws SQLException {
		LOGGER.log(Level.INFO, "Called getImportedKeys()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public ResultSet getIndexInfo(final String catalog, final String schema, final String table, final boolean unique,
			final boolean approximate) throws SQLException {
		// we only have non-unique indices
		// and don't report index statistics so have no use for approximate
		LOGGER.log(Level.INFO, "Called getIndexInfo()");
		boolean test = false;
		String wireSchema = "";
		if (schema != null)
		{
			test = true;
			wireSchema = schema;
		}
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_INDEX_INFO, wireSchema, table, "", test);
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		LOGGER.log(Level.INFO, "Called getJDBCMajorVersion()");
		return 4;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		LOGGER.log(Level.INFO, "Called getJDBCMinorVersion()");
		return 1;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxBinaryLiteralLength()");
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxCatalogNameLength()");
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxCharLiteralLength()");
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxColumnNameLength()");
		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxColumnsInGroupBy()");
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxColumnsInIndex()");
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxColumnsInOrderBy()");
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxColumnsInSelect()");
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxColumnsInTable()");
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxConnections()");
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxCursorNameLength()");
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxIndexLength()");
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxProcedureNameLength()");
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxRowSize()");
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxSchemaNameLength()");
		return 0;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxStatementLength()");
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxStatements()");
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxTableNameLength()");
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxTablesInSelect()");
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		LOGGER.log(Level.INFO, "Called getMaxUserNameLength()");
		return 0;
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		LOGGER.log(Level.INFO, "Called getNumericFunctions()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataString(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_NUMERIC_FUNCTIONS);
	}

	@Override
	public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table) throws SQLException {
		LOGGER.log(Level.INFO, "Called getPrimaryKeys()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public ResultSet getProcedureColumns(final String catalog, final String schemaPattern,
			final String procedureNamePattern, final String columnNamePattern) throws SQLException {
		LOGGER.log(Level.INFO, "Called getProcedureColumns()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public ResultSet getProcedures(final String catalog, final String schemaPattern, final String procedureNamePattern)
			throws SQLException {
		LOGGER.log(Level.INFO, "Called getProcedures()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		LOGGER.log(Level.INFO, "Called getProcedureTerm()");
		return "stored procedure";
	}

	@Override
	public ResultSet getPseudoColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
			final String columnNamePattern) throws SQLException {
		LOGGER.log(Level.INFO, "Called getPseudoColumns()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		LOGGER.log(Level.INFO, "Called getResultSetHoldability()");
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		LOGGER.log(Level.INFO, "Called getRowIdLifetime()");
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		LOGGER.log(Level.INFO, "Called getSchemas()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_SCHEMAS, "", "", "", false);
	}

	@Override
	public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
		LOGGER.log(Level.INFO, "Called getSchemas()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_SCHEMAS, schemaPattern, "", "", true);
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		LOGGER.log(Level.INFO, "Called getSchemaTerm()");
		return "schema";
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		LOGGER.log(Level.INFO, "Called getSearchStringEscape()");
		return "\\";
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		LOGGER.log(Level.INFO, "Called getSQLKeywords()");
		return ((XGStatement) conn.createStatement())
				.fetchSystemMetadataString(ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_SQL_KEYWORDS);
	}

	@Override
	public int getSQLStateType() throws SQLException {
		LOGGER.log(Level.INFO, "Called getSQLStateType()");
		return DatabaseMetaData.sqlStateSQL;
	}

	@Override
	public String getStringFunctions() throws SQLException {
		LOGGER.log(Level.INFO, "Called getStringFunctions()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataString(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_STRING_FUNCTIONS);
	}

	@Override
	public ResultSet getSuperTables(final String catalog, final String schemaPattern, final String tableNamePattern)
			throws SQLException {
		LOGGER.log(Level.INFO, "Called getSuperTables()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public ResultSet getSuperTypes(final String catalog, final String schemaPattern, final String typeNamePattern)
			throws SQLException {
		LOGGER.log(Level.INFO, "Called getSuperTypes()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		LOGGER.log(Level.INFO, "Called getSystemFunctions()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataString(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_SYSTEM_FUNCTIONS);
	}

	@Override
	public ResultSet getTablePrivileges(final String catalog, final String schemaPattern, final String tableNamePattern)
			throws SQLException {
		LOGGER.log(Level.INFO, "Called getTablePrivileges()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public ResultSet getTables(final String catalog, final String schemaPattern, final String tableNamePattern,
			final String[] types) throws SQLException {
		// we only have one table type
		LOGGER.log(Level.INFO, "Called getTables()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_TABLES, schemaPattern, tableNamePattern,
				"", true);
	}
	
	public ResultSet getSystemTables(final String catalog, final String schemaPattern, final String tableNamePattern,
			final String[] types) throws SQLException {
		// we only have one table type
		LOGGER.log(Level.INFO, "Called getSystemTables()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_SYSTEM_TABLES, schemaPattern, tableNamePattern,
				"", true);
	}
		
	public ResultSet getViews(final String catalog, final String schemaPattern, final String viewNamePattern, 
			final String[] types) throws SQLException {
		LOGGER.log(Level.INFO, "Called getViews()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_VIEWS, schemaPattern, viewNamePattern, 
				"", true); 
	}
		
	@Override
	public ResultSet getTableTypes() throws SQLException {
		LOGGER.log(Level.WARNING, "getTableTypes() was called, which is not supported");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		LOGGER.log(Level.INFO, "Called getTimeDateFunctions()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataString(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_TIME_DATE_FUNCTIONS);
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		LOGGER.log(Level.INFO, "Called getTypeInfo()");
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_TYPE_INFO, "", "", "", true);
	}

	@Override
	public ResultSet getUDTs(final String catalog, final String schemaPattern, final String typeNamePattern,
			final int[] types) throws SQLException {
		LOGGER.log(Level.INFO, "Called getUDTs()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public String getURL() throws SQLException {
		LOGGER.log(Level.INFO, "Called getURL()");
		return ((XGConnection) conn).getURL();
	}

	@Override
	public String getUserName() throws SQLException {
		LOGGER.log(Level.INFO, "Called getUserName()");
		return ((XGConnection) conn).getUser();
	}

	@Override
	public ResultSet getVersionColumns(final String catalog, final String schema, final String table)
			throws SQLException {
		LOGGER.log(Level.INFO, "Called getVersionColumns()");
		return new XGResultSet((XGConnection)conn, new ArrayList<Object>(), (XGStatement)conn.createStatement());
	}

	@Override
	public boolean insertsAreDetected(final int type) throws SQLException {
		LOGGER.log(Level.INFO, "Called insertsAreDetected()");
		return false;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		LOGGER.log(Level.INFO, "Called isCatalogAtStart()");
		return true;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		LOGGER.log(Level.INFO, "Called isReadOnly()");
		return false;
	}

	@Override
	public boolean isWrapperFor(final Class<?> iface) throws SQLException {
		LOGGER.log(Level.INFO, "Called isWrapperFor()");
		return false;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		LOGGER.log(Level.INFO, "Called locatorsUpdateCopy()");
		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		LOGGER.log(Level.INFO, "Called nullPlusNonNullIsNull()");
		return true;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		LOGGER.log(Level.INFO, "Called nullsAreSortedAtEnd()");
		return true;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		LOGGER.log(Level.INFO, "Called nullsAreSortedAtStart()");
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		LOGGER.log(Level.INFO, "Called nullsAreSortedHigh()");
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		LOGGER.log(Level.INFO, "Called nullsAreSortedLow()");
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(final int type) throws SQLException {
		LOGGER.log(Level.INFO, "Called othersDeletesAreVisible()");
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(final int type) throws SQLException {
		LOGGER.log(Level.INFO, "Called othersInsertsAreVisible()");
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(final int type) throws SQLException {
		LOGGER.log(Level.INFO, "Called othersUpdatesAreVisible()");
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(final int type) throws SQLException {
		LOGGER.log(Level.INFO, "Called ownDeletesAreVisible()");
		return true;
	}

	@Override
	public boolean ownInsertsAreVisible(final int type) throws SQLException {
		LOGGER.log(Level.INFO, "Called ownInsertsAreVisible()");
		return true;
	}

	@Override
	public boolean ownUpdatesAreVisible(final int type) throws SQLException {
		LOGGER.log(Level.INFO, "Called ownUpdatesAreVisible()");
		return true;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		LOGGER.log(Level.INFO, "Called storesLowerCaseIdentifiers()");
		return true;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		LOGGER.log(Level.INFO, "Called storesLowerCaseQuotedIdentifiers()");
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		LOGGER.log(Level.INFO, "Called storesMixedCaseIdentifiers()");
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		LOGGER.log(Level.INFO, "Called storesMixedCaseQuotedIdentifiers()");
		return true;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		LOGGER.log(Level.INFO, "Called storesUpperCaseIdentifiers()");
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		LOGGER.log(Level.INFO, "Called storesUpperCaseQuotedIdentifiers()");
		return false;
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsAlterTableWithAddColumn()");
		return true;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsAlterTableWithDropColumn()");
		return true;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsANSI92EntryLevelSQL()");
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsANSI92FullSQL()");
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsANSI92IntermediateSQL()");
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsBatchUpdates()");
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsCatalogsInDataManipulation()");
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsCatalogsInIndexDefinitions()");
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsCatalogsInPrivilegeDefinitions()");
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsCatalogsInProcedureCalls()");
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsCatalogsInTableDefinitions()");
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsColumnAliasing()");
		return true;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsConvert()");
		return false;
	}

	@Override
	public boolean supportsConvert(final int fromType, final int toType) throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsConvert()");
		return false;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsCoreSQLGrammar()");
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsCorrelatedSubqueries()");
		return true;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsDataDefinitionAndDataManipulationTransactions()");
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsDataManipulationTransactionsOnly()");
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsDifferentTableCorrelationNames()");
		return true;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsExpressionsInOrderBy()");
		return true;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsExtendedSQLGrammar()");
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsFullOuterJoins()");
		return true;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsGetGeneratedKeys()");
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsGroupBy()");
		return true;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsGroupByBeyondSelect()");
		return true;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsGroupByUnrelated()");
		return true;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsIntegrityEnhancementFacility()");
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsLikeEscapeClause()");
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsLikeEscapeClause()");
		return true;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsMinimumSQLGrammar()");
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsMixedCaseIdentifiers()");
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsMixedCaseQuotedIdentifiers()");
		return true;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsMultipleOpenResults()");
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsMultipleResultSets()");
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsMultipleTransactions()");
		return true;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsNamedParameters()");
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsNonNullableColumns()");
		return true;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsOpenCursorsAcrossCommit()");
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsOpenCursorsAcrossRollback()");
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsOpenStatementsAcrossCommit()");
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsOpenStatementsAcrossRollback()");
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsOrderByUnrelated()");
		return true;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsOuterJoins()");
		return true;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsPositionedDelete()");
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsPositionedUpdate()");
		return false;
	}

	@Override
	public boolean supportsResultSetConcurrency(final int type, final int concurrency) throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsResultSetConcurrency()");
		if (type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY)
		{
			return true;
		}

		return false;
	}

	@Override
	public boolean supportsResultSetHoldability(final int holdability) throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsResultSetHoldability()");
		if (holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT)
		{
			return true;
		}

		return false;
	}

	@Override
	public boolean supportsResultSetType(final int type) throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsResultSetType()");
		if (type == ResultSet.TYPE_FORWARD_ONLY)
		{
			return true;
		}

		return false;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsSavepoints()");
		return false;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsSchemasInDataManipulation()");
		return true;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsSchemasInIndexDefinitions()");
		return true;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsSchemasInPrivilegeDefinitions()");
		return true;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsSchemasInProcedureCalls()");
		return true;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsSchemasInTableDefinitions()");
		return true;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsSelectForUpdate()");
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsStatementPooling()");
		return false;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsStoredFunctionsUsingCallSyntax()");
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsStoredProcedures()");
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsSubqueriesInComparisons()");
		return true;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsSubqueriesInExists()");
		return true;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsSubqueriesInIns()");
		return true;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsSubqueriesInQuantifieds()");
		return true;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsTableCorrelationNames()");
		return true;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(final int level) throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsTransactionIsolationLevel()");
		if (level == Connection.TRANSACTION_NONE)
		{
			return true;
		}

		return false;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsTransactions()");
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsUnion()");
		return true;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		LOGGER.log(Level.INFO, "Called supportsUnionAll()");
		return true;
	}

	@Override
	public <T> T unwrap(final Class<T> iface) throws SQLException {
		LOGGER.log(Level.WARNING, "unwrap() was called, which is not supported");
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean updatesAreDetected(final int type) throws SQLException {
		LOGGER.log(Level.INFO, "Called updatesAreDetected()");
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		LOGGER.log(Level.INFO, "Called usesLocalFilePerTable()");
		return false;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		LOGGER.log(Level.INFO, "Called usesLocalFiles()");
		return false;
	}

}
