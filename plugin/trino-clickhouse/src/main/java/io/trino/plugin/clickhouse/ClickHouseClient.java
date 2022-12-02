/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.clickhouse;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.clickhouse.client.data.ClickHouseObjectValue;
import com.clickhouse.client.data.array.ClickHouseDoubleArrayValue;
import com.clickhouse.client.data.array.ClickHouseLongArrayValue;
import com.clickhouse.jdbc.ClickHouseArray;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.plugin.base.util.JsonTypeUtil;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.trino.plugin.jdbc.expression.AggregateFunctionRule;
import io.trino.plugin.jdbc.expression.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.expression.ImplementCount;
import io.trino.plugin.jdbc.expression.ImplementCountAll;
import io.trino.plugin.jdbc.expression.ImplementMinMax;
import io.trino.plugin.jdbc.expression.ImplementSum;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.type.*;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.wrappedLongArray;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.*;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.ARRAY;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Long.reverseBytes;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.DatabaseMetaData.columnNoNulls;

public class ClickHouseClient
        extends BaseJdbcClient
{
    static final int CLICKHOUSE_MAX_DECIMAL_PRECISION = 76;

    private final boolean mapStringAsVarchar;
    private final AggregateFunctionRewriter aggregateFunctionRewriter;
    private final Type uuidType;
    private final Type jsonType;

    @Inject
    public ClickHouseClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            TypeManager typeManager,
            ClickHouseConfig clickHouseConfig,
            IdentifierMapping identifierMapping)
    {
        super(config, "\"", connectionFactory, identifierMapping);
        this.jsonType = typeManager.getType(new TypeSignature(JSON));
        this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));
        // TODO (https://github.com/trinodb/trino/issues/7102) define session property
        this.mapStringAsVarchar = clickHouseConfig.isMapStringAsVarchar();
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter(
                this::quoted,
                ImmutableSet.<AggregateFunctionRule>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax())
                        .add(new ImplementSum(ClickHouseClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
                        .build());
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.DECIMAL, Optional.of("Decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    protected String quoted(@Nullable String catalog, @Nullable String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
    }

    @Override
    protected void copyTableSchema(Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        // ClickHouse does not support `create table tbl as select * from tbl2 where 0=1`
        // ClickHouse supports the following two methods to copy schema
        // 1. create table tbl as tbl2
        // 2. create table tbl1 ENGINE=<engine> as select * from tbl2
        String sql = format(
                "CREATE TABLE %s AS %s ",
                quoted(null, schemaName, newTableName),
                quoted(null, schemaName, tableName));
        execute(connection, sql);
    }

    @Override
    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
        Map<String, Object> tableProperties = tableMetadata.getProperties();
        ClickHouseEngineType engine = ClickHouseTableProperties.getEngine(tableProperties);
        tableOptions.add("ENGINE = " + engine.getEngineType());
        if (engine == ClickHouseEngineType.MERGETREE && formatProperty(ClickHouseTableProperties.getOrderBy(tableProperties)).isEmpty()) {
            // order_by property is required
            throw new TrinoException(INVALID_TABLE_PROPERTY,
                    format("The property of %s is required for table engine %s", ClickHouseTableProperties.ORDER_BY_PROPERTY, engine.getEngineType()));
        }
        formatProperty(ClickHouseTableProperties.getOrderBy(tableProperties)).ifPresent(value -> tableOptions.add("ORDER BY " + value));
        formatProperty(ClickHouseTableProperties.getPrimaryKey(tableProperties)).ifPresent(value -> tableOptions.add("PRIMARY KEY " + value));
        formatProperty(ClickHouseTableProperties.getPartitionBy(tableProperties)).ifPresent(value -> tableOptions.add("PARTITION BY " + value));
        ClickHouseTableProperties.getSampleBy(tableProperties).ifPresent(value -> tableOptions.add("SAMPLE BY " + value));

        return format("CREATE TABLE %s (%s) %s", quoted(remoteTableName), join(", ", columns), join(" ", tableOptions.build()));
    }

    @Override
    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        StringBuilder sb = new StringBuilder()
                .append(quoted(columnName))
                .append(" ");
        if (column.isNullable()) {
            // set column nullable property explicitly
            sb.append("Nullable(").append(toWriteMapping(session, column.getType()).getDataType()).append(")");
        }
        else {
            // By default, the clickhouse column is not allowed to be null
            sb.append(toWriteMapping(session, column.getType()).getDataType());
        }
        return sb.toString();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        execute(session, "CREATE DATABASE " + quoted(schemaName));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        execute(session, "DROP DATABASE " + quoted(schemaName));
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String remoteColumnName = getIdentifierMapping().toRemoteColumnName(connection, column.getName());
            String sql = format(
                    "ALTER TABLE %s ADD COLUMN %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    getColumnDefinitionSql(session, column, remoteColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String newRemoteColumnName = getIdentifierMapping().toRemoteColumnName(connection, newColumnName);
            String sql = format("ALTER TABLE %s RENAME COLUMN %s TO %s ",
                    quoted(handle.getRemoteTableName()),
                    quoted(jdbcColumn.getColumnName()),
                    quoted(newRemoteColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // ClickHouse maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                null,
                schemaName.orElse(null),
                escapeNamePattern(tableName, metadata.getSearchStringEscape()).orElse(null),
                new String[] {"TABLE", "VIEW"});
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        String sql = "DROP TABLE " + quoted(handle.getRemoteTableName());
        execute(session, sql);
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        String sql = format("RENAME TABLE %s.%s TO %s.%s",
                quoted(schemaName),
                quoted(tableName),
                quoted(newTable.getSchemaName()),
                quoted(newTable.getTableName()));
        execute(session, sql);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        if (typeName.startsWith("Nullable(")){
            typeName = StrUtil.removePrefix(typeName,"Nullable(");
            typeName = StrUtil.removeSuffix(typeName,")");
        }

        String jdbcTypeName = typeName.replaceAll("\\(.*\\)$", "");
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        switch (jdbcTypeName) {
            case "IPv4":
            case "IPv6":
                // TODO (https://github.com/trinodb/trino/issues/7098) map to Trino IPADDRESS
            case "Enum8":
            case "Enum16":
                return Optional.of(ColumnMapping.sliceMapping(
                        createUnboundedVarcharType(),
                        varcharReadFunction(createUnboundedVarcharType()),
                        varcharWriteFunction(),
                        // TODO (https://github.com/trinodb/trino/issues/7100) Currently pushdown would not work and may require a custom bind expression
                        DISABLE_PUSHDOWN));

            case "FixedString": // FixedString(n)
            case "String":
                if (mapStringAsVarchar) {
                    return Optional.of(ColumnMapping.sliceMapping(
                            createUnboundedVarcharType(),
                            varcharReadFunction(createUnboundedVarcharType()),
                            varcharWriteFunction(),
                            DISABLE_PUSHDOWN));
                }
                // TODO (https://github.com/trinodb/trino/issues/7100) test & enable predicate pushdown
                return Optional.of(varbinaryColumnMapping());
            case "UUID":
                return Optional.of(uuidColumnMapping());
            case "UInt64":
                return Optional.of(bigintColumnMapping());
        }

        if(StrUtil.startWithIgnoreCase(typeName,"DateTime")) {
            return Optional.of(timestampColumnMappingUsingSqlTimestampWithRounding(TIMESTAMP_MILLIS));
        }

        String typeNameJson = typeName.toLowerCase(Locale.ROOT);
        if(typeNameJson.startsWith("json") || typeNameJson.startsWith("array") || typeNameJson.startsWith("map")) {
            return Optional.of(jsonColumnMapping(typeName));
        }

        switch (typeHandle.getJdbcType()) {
            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.FLOAT:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.DECIMAL:
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                int precision = typeHandle.getRequiredColumnSize();

                ColumnMapping decimalColumnMapping;
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = Math.min(decimalDigits, getDecimalDefaultScale(session));
                    decimalColumnMapping = decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session));
                }
                else {
                    decimalColumnMapping = decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)));
                }
                return Optional.of(new ColumnMapping(
                        decimalColumnMapping.getType(),
                        decimalColumnMapping.getReadFunction(),
                        decimalColumnMapping.getWriteFunction(),
                        // TODO (https://github.com/trinodb/trino/issues/7100) fix, enable and test decimal pushdown
                        DISABLE_PUSHDOWN));

            case Types.DATE:
                return Optional.of(dateColumnMapping());

            case Types.TIMESTAMP:
                // clickhouse not implemented for type=class java.time.LocalDateTime
                // TODO replace it using timestamp relative function after clickhouse adds support for LocalDateTime, or use UTC Calendar
                return Optional.of(timestampColumnMappingUsingSqlTimestampWithRounding(TIMESTAMP_MILLIS));
            default:
                System.out.println("--------暂不支持---------" + typeHandle.getJdbcTypeName());
        }

        return Optional.empty();
    }

    public static SliceWriteFunction varcharWriteFunction()
    {
        return (statement, index, value) ->  statement.setString(index, value.toStringUtf8());
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            // ClickHouse is no separate type for boolean values. Use UInt8 type, restricted to the values 0 or 1.
            return WriteMapping.booleanMapping("UInt8", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("Int8", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("Int16", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("Int32", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("Int64", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("Float32", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("Float64", doubleWriteFunction());
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("Decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.sliceMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType || type instanceof VarcharType) {
            // The String type replaces the types VARCHAR, BLOB, CLOB, and others from other DBMSs.
            return WriteMapping.sliceMapping("String", varcharWriteFunction());
        }
        if (type instanceof VarbinaryType) {
            // Strings of an arbitrary length. The length is not limited
            return WriteMapping.sliceMapping("String", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("Date", dateWriteFunction());
        }
        if (type.equals(uuidType)) {
            return WriteMapping.sliceMapping("UUID", uuidWriteFunction());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (tableHandle.getColumns().isPresent()) {
            return tableHandle.getColumns().get();
        }
        checkArgument(tableHandle.isNamedRelation(), "Cannot get columns for %s", tableHandle);
        SchemaTableName schemaTableName = tableHandle.getRequiredNamedRelation().getSchemaTableName();
        RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();

        try (Connection connection = connectionFactory.openConnection(session);
             ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
            int allColumns = 0;
            List<JdbcColumnHandle> columns = new ArrayList<>();
            while (resultSet.next()) {
                // skip if table doesn't match expected
                if (!(Objects.equals(remoteTableName, getRemoteTable(resultSet)))) {
                    continue;
                }
                allColumns++;
                String columnName = resultSet.getString("COLUMN_NAME");
                JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                        getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null")),
                        Optional.ofNullable(resultSet.getString("TYPE_NAME")),
                        getInteger(resultSet, "COLUMN_SIZE"),
                        getInteger(resultSet, "DECIMAL_DIGITS"),
                        Optional.empty(),
                        Optional.empty());
                Optional<ColumnMapping> columnMapping = toColumnMapping(session, connection, typeHandle);
                // skip unsupported column types

                // Note: some databases (e.g. SQL Server) do not return column remarks/comment here.
                Optional<String> comment = Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
                if (columnMapping.isPresent()) {
                    columns.add(JdbcColumnHandle.builder()
                            .setColumnName(columnName)
                            .setJdbcTypeHandle(typeHandle)
                            .setColumnType(columnMapping.get().getType())
                            .setComment(comment)
                            .build());
                }
                if (columnMapping.isEmpty()) {
                    UnsupportedTypeHandling unsupportedTypeHandling = getUnsupportedTypeHandling(session);
                    verify(
                            unsupportedTypeHandling == IGNORE,
                            "Unsupported type handling is set to %s, but toTrinoType() returned empty for %s",
                            unsupportedTypeHandling,
                            typeHandle);
                }
            }
            if (columns.isEmpty()) {
                // A table may have no supported columns. In rare cases (e.g. PostgreSQL) a table might have no columns at all.
                throw new TableNotFoundException(
                        schemaTableName,
                        format("Table '%s' has no supported columns (all %s columns are not supported)", schemaTableName, allColumns));
            }
            return ImmutableList.copyOf(columns);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private static RemoteTableName getRemoteTable(ResultSet resultSet)
            throws SQLException
    {
        return new RemoteTableName(
                Optional.ofNullable(resultSet.getString("TABLE_CAT")),
                Optional.ofNullable(resultSet.getString("TABLE_SCHEM")),
                resultSet.getString("TABLE_NAME"));
    }

    /**
     * format property to match ClickHouse create table statement
     *
     * @param prop property will be formatted
     * @return formatted property
     */
    private Optional<String> formatProperty(List<String> prop)
    {
        if (prop == null || prop.isEmpty()) {
            return Optional.empty();
        }
        else if (prop.size() == 1) {
            // only one column
            return Optional.of(prop.get(0));
        }
        else {
            // include more than one columns
            return Optional.of("(" + String.join(",", prop) + ")");
        }
    }

    private ColumnMapping uuidColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                uuidType,
                (resultSet, columnIndex) -> uuidSlice((UUID) resultSet.getObject(columnIndex)),
                uuidWriteFunction());
    }

    // see io.trino.type.UuidType#getObjectValue
    private static SliceWriteFunction uuidWriteFunction()
    {
        return (statement, index, value) -> {
            long high = reverseBytes(value.getLong(0));
            long low = reverseBytes(value.getLong(SIZE_OF_LONG));
            UUID uuid = new UUID(high, low);
            statement.setObject(index, uuid, Types.OTHER);
        };
    }

    private static Slice uuidSlice(UUID uuid)
    {
        return wrappedLongArray(
                reverseBytes(uuid.getMostSignificantBits()),
                reverseBytes(uuid.getLeastSignificantBits()));
    }

    public static Object deserialize(Object object)
    {
        try {
            String json = JSONUtil.toJsonStr(object);
            if (JSONUtil.isTypeJSONObject(json)){
                return deserializeJSONObject(json);
            }else if (JSONUtil.isTypeJSONArray(json)){
                return deserializeJSONArray(json);
            }else {
                return json;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Map<String, Object> deserializeJSONObject(Object object)
    {
        return JSONUtil.parseObj(object);
    }

    public static List<Object> deserializeJSONArray(Object object)
    {
        return JSONUtil.parseArray(object);
    }

    private ColumnMapping jsonColumnMapping(String jsonTypeName)
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> {
                    Object jsonObject = resultSet.getObject(columnIndex);
                    Object deserialize;
                    if (jsonObject instanceof ClickHouseArray) {
                        deserialize = deserialize(resultSet.getString(columnIndex));
                    }else {
                        deserialize = deserialize(jsonObject);
                    }
                    try {
                        return JsonTypeUtil.toJsonValue(deserialize);
                    } catch (IOException e) {
                        throw new SQLException("无法解析", e);
                    }

                },
                (statement, index, value) -> {
                    if (StrUtil.containsIgnoreCase(jsonTypeName,"array")){
                        JSONArray list = JSONUtil.parseArray(value.toStringUtf8());
                        Array arrayOf;
                        if (StrUtil.containsIgnoreCase(jsonTypeName, "float")){
                            double[] data = (double[]) list.toArray(double.class);
                            ClickHouseObjectValue<double[]> of = ClickHouseDoubleArrayValue.of(data);
                            arrayOf = statement.getConnection().createArrayOf(jsonTypeName, of.asArray());
                        }else if (StrUtil.containsIgnoreCase(jsonTypeName, "int")){
                            long[] data = (long[]) list.toArray(long.class);
                            ClickHouseObjectValue<long[]> of = ClickHouseLongArrayValue.of(data);
                            arrayOf = statement.getConnection().createArrayOf(jsonTypeName, of.asArray());
                        }else {
                            arrayOf = statement.getConnection().createArrayOf(jsonTypeName, list.toArray());
                        }
                        statement.setArray(index, arrayOf);
                    }else if (StrUtil.containsIgnoreCase(jsonTypeName,"map")){
                        Map data = JSONUtil.parseObj(value.toStringUtf8()).toBean(Map.class);
                        statement.setObject(index, data);
                    }else {
                        throw new SQLException("暂不支持");
                    }
                },
                DISABLE_PUSHDOWN);
    }
}
