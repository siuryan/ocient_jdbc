package com.ocient.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import com.ocient.jdbc.proto.ClientWireProtocol;

public class XGDatabaseMetaData implements DatabaseMetaData
{
	private final Connection conn;

	public XGDatabaseMetaData(final Connection conn)
	{
		this.conn = conn;
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return true;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean deletesAreDetected(final int type) throws SQLException {
		return false;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return true;
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getAttributes(final String catalog, final String schemaPattern, final String typeNamePattern,
			final String attributeNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getBestRowIdentifier(final String catalog, final String schema, final String table,
			final int scope, final boolean nullable) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		return ".";
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		return "system";
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getColumnPrivileges(final String catalog, final String schema, final String table,
			final String columnNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
			final String columnNamePattern) throws SQLException {
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_COLUMNS, schemaPattern, tableNamePattern,
				columnNamePattern, true);
	}

	@Override
	public Connection getConnection() throws SQLException {
		return conn;
	}

	@Override
	public ResultSet getCrossReference(final String parentCatalog, final String parentSchema, final String parentTable,
			final String foreignCatalog, final String foreignSchema, final String foreignTable) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataInt(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_DATABASE_MAJOR_VERSION);
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataInt(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_DATABASE_MINOR_VERSION);
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return "Ocient";
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataString(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_DATABASE_PRODUCT_VERSION);
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public int getDriverMajorVersion() {
		return ((XGConnection) conn).getMajorVersion();
	}

	@Override
	public int getDriverMinorVersion() {
		return ((XGConnection) conn).getMinorVersion();
	}

	@Override
	public String getDriverName() throws SQLException {
		return "Ocient JDBC Driver";
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return ((XGConnection) conn).getVersion();
	}

	@Override
	public ResultSet getExportedKeys(final String catalog, final String schema, final String table)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		return "";
	}

	@Override
	public ResultSet getFunctionColumns(final String catalog, final String schemaPattern,
			final String functionNamePattern, final String columnNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getFunctions(final String catalog, final String schemaPattern, final String functionNamePattern)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return "'";
	}

	@Override
	public ResultSet getImportedKeys(final String catalog, final String schema, final String table)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getIndexInfo(final String catalog, final String schema, final String table, final boolean unique,
			final boolean approximate) throws SQLException {
		// we only have non-unique indices
		// and don't report index statistics so have no use for approximate
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
		return 4;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return 1;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		return 0;
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataString(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_NUMERIC_FUNCTIONS);
	}

	@Override
	public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getProcedureColumns(final String catalog, final String schemaPattern,
			final String procedureNamePattern, final String columnNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getProcedures(final String catalog, final String schemaPattern, final String procedureNamePattern)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		return "stored procedure";
	}

	@Override
	public ResultSet getPseudoColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
			final String columnNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_SCHEMAS, "", "", "", false);
	}

	@Override
	public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_SCHEMAS, schemaPattern, "", "", true);
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return "schema";
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return "";
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		return ((XGStatement) conn.createStatement())
				.fetchSystemMetadataString(ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_SQL_KEYWORDS);
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return DatabaseMetaData.sqlStateSQL;
	}

	@Override
	public String getStringFunctions() throws SQLException {
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataString(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_STRING_FUNCTIONS);
	}

	@Override
	public ResultSet getSuperTables(final String catalog, final String schemaPattern, final String tableNamePattern)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getSuperTypes(final String catalog, final String schemaPattern, final String typeNamePattern)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataString(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_SYSTEM_FUNCTIONS);
	}

	@Override
	public ResultSet getTablePrivileges(final String catalog, final String schemaPattern, final String tableNamePattern)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getTables(final String catalog, final String schemaPattern, final String tableNamePattern,
			final String[] types) throws SQLException {
		// we only have one table type
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_TABLES, schemaPattern, tableNamePattern,
				"", true);
	}
	
	public ResultSet getSystemTables(final String catalog, final String schemaPattern, final String tableNamePattern,
			final String[] types) throws SQLException {
		// we only have one table type
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_SYSTEM_TABLES, schemaPattern, tableNamePattern,
				"", true);
	}
		
	public ResultSet getViews(final String catalog, final String schemaPattern, final String viewNamePattern, 
			final String[] types) throws SQLException {
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_VIEWS, schemaPattern, viewNamePattern, 
				"", true); 
	}
		
	@Override
	public ResultSet getTableTypes() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataString(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_TIME_DATE_FUNCTIONS);
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		return ((XGStatement) conn.createStatement()).fetchSystemMetadataResultSet(
				ClientWireProtocol.FetchSystemMetadata.SystemMetadataCall.GET_TYPE_INFO, "", "", "", true);
	}

	@Override
	public ResultSet getUDTs(final String catalog, final String schemaPattern, final String typeNamePattern,
			final int[] types) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getURL() throws SQLException {
		return ((XGConnection) conn).getURL();
	}

	@Override
	public String getUserName() throws SQLException {
		return ((XGConnection) conn).getUser();
	}

	@Override
	public ResultSet getVersionColumns(final String catalog, final String schema, final String table)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean insertsAreDetected(final int type) throws SQLException {
		return false;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return true;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean isWrapperFor(final Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return true;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return true;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(final int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(final int type) throws SQLException {
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(final int type) throws SQLException {
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(final int type) throws SQLException {
		return true;
	}

	@Override
	public boolean ownInsertsAreVisible(final int type) throws SQLException {
		return true;
	}

	@Override
	public boolean ownUpdatesAreVisible(final int type) throws SQLException {
		return true;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert(final int fromType, final int toType) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsResultSetConcurrency(final int type, final int concurrency) throws SQLException {
		if (type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY)
		{
			return true;
		}

		return false;
	}

	@Override
	public boolean supportsResultSetHoldability(final int holdability) throws SQLException {
		if (holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT)
		{
			return true;
		}

		return false;
	}

	@Override
	public boolean supportsResultSetType(final int type) throws SQLException {
		if (type == ResultSet.TYPE_FORWARD_ONLY)
		{
			return true;
		}

		return false;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(final int level) throws SQLException {
		if (level == Connection.TRANSACTION_NONE)
		{
			return true;
		}

		return false;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return true;
	}

	@Override
	public <T> T unwrap(final Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean updatesAreDetected(final int type) throws SQLException {
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return false;
	}

}
