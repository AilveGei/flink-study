package org.apache.flink.connector.jdbc.table;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @Author ml.wang
 * @Date 2023-07-28
 */
public class JdbcDynamicTableSource
        implements ScanTableSource,
        LookupTableSource,
        SupportsFilterPushDown,
        SupportsProjectionPushDown,
        SupportsLimitPushDown {

    private final org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions options;
    private final JdbcReadOptions readOptions;
    private final JdbcLookupOptions lookupOptions;
    private DataType physicalRowDataType;
    private final String dialectName;
    private long limit = -1L;

    private List<ResolvedExpression> filters;

    public JdbcDynamicTableSource(JdbcConnectorOptions options, JdbcReadOptions readOptions, JdbcLookupOptions lookupOptions, DataType physicalRowDataType) {
        this.options = options;
        this.readOptions = readOptions;
        this.lookupOptions = lookupOptions;
        this.physicalRowDataType = physicalRowDataType;
        this.dialectName = options.getDialect().dialectName();
    }

    public LookupTableSource.LookupRuntimeProvider getLookupRuntimeProvider(LookupTableSource.LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];

        for(int i = 0; i < keyNames.length; ++i) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = (String)DataType.getFieldNames(this.physicalRowDataType).get(innerKeyArr[0]);
        }

        RowType rowType = (RowType)this.physicalRowDataType.getLogicalType();
        return TableFunctionProvider.of(new JdbcRowDataLookupFunction(this.options, this.lookupOptions, (String[])DataType.getFieldNames(this.physicalRowDataType).toArray(new String[0]), (DataType[])DataType.getFieldDataTypes(this.physicalRowDataType).toArray(new DataType[0]), keyNames, rowType));
    }

    public ScanTableSource.ScanRuntimeProvider getScanRuntimeProvider(ScanTableSource.ScanContext runtimeProviderContext) {
        JdbcRowDataInputFormat.Builder builder = JdbcRowDataInputFormat.builder()
                .setDrivername(this.options.getDriverName())
                .setDBUrl(this.options.getDbURL())
                .setUsername((String)this.options.getUsername().orElse(null))
                .setPassword((String)this.options.getPassword().orElse(null))
                .setAutoCommit(this.readOptions.getAutoCommit());
        if (this.readOptions.getFetchSize() != 0) {
            builder.setFetchSize(this.readOptions.getFetchSize());
        }

        JdbcDialect dialect = this.options.getDialect();
        String query = dialect.getSelectFromStatement(
                this.options.getTableName(),
                (String[])DataType.getFieldNames(this.physicalRowDataType).toArray(new String[0]),
                new String[0]);
        if (this.readOptions.getPartitionColumnName().isPresent()) {
            long lowerBound = (Long)this.readOptions.getPartitionLowerBound().get();
            long upperBound = (Long)this.readOptions.getPartitionUpperBound().get();
            int numPartitions = (Integer)this.readOptions.getNumPartitions().get();
            builder.setParametersProvider((new JdbcNumericBetweenParametersProvider(lowerBound, upperBound)).ofBatchNum(numPartitions));
            query = query + " WHERE " + dialect.quoteIdentifier((String)this.readOptions.getPartitionColumnName().get()) + " BETWEEN ? AND ?";
        }

        if (this.limit >= 0L) {
            query = String.format("%s %s", query, dialect.getLimitClause(this.limit));
        }

        builder.setQuery(query);
        RowType rowType = (RowType)this.physicalRowDataType.getLogicalType();
        builder.setRowConverter(dialect.getRowConverter(rowType));
        builder.setRowDataTypeInfo(runtimeProviderContext.createTypeInformation(this.physicalRowDataType));
        return InputFormatProvider.of(builder.build());
    }

    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    public boolean supportsNestedProjection() {
        return false;
    }

    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.physicalRowDataType = Projection.of(projectedFields).project(this.physicalRowDataType);
    }

    public DynamicTableSource copy() {
        return new JdbcDynamicTableSource(this.options, this.readOptions, this.lookupOptions, this.physicalRowDataType);
    }

    public String asSummaryString() {
        return "JDBC:" + this.dialectName;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof JdbcDynamicTableSource)) {
            return false;
        } else {
            JdbcDynamicTableSource that = (JdbcDynamicTableSource)o;
            return Objects.equals(this.options, that.options) && Objects.equals(this.readOptions, that.readOptions) && Objects.equals(this.lookupOptions, that.lookupOptions) && Objects.equals(this.physicalRowDataType, that.physicalRowDataType) && Objects.equals(this.dialectName, that.dialectName) && Objects.equals(this.limit, that.limit);
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.options, this.readOptions, this.lookupOptions, this.physicalRowDataType, this.dialectName, this.limit});
    }

    public void applyLimit(long limit) {
        this.limit = limit;
    }

    /**
     * 1. 该方法的作用是将filters(过滤条件)应用到当前的DynamicTableSource上
     * 2. 该方法的返回值是一个Result对象，该对象包含两个List<ResolvedExpression>类型的属性，
     *    分别是acceptedFilters(接受的过滤条件)和remainingFilters(剩余的过滤条件)
     * 3. acceptedFilters是指那些被DynamicTableSource使用的filters，
     *    但是这些filters可能只是尽可能在原基础上被应用，
     *    因此，这些filters的信息可以帮助planner来调整当前plan的成本估算
     * 4. remainingFilters是指那些不能被DynamicTableSource完全消费的filters，
     *    这些filters决定了是否需要在runtime阶段继续应用filter操作
     * 5. acceptedFilters和remainingFilters不能是disjunctive lists，
     *    一个filter可以同时出现在这两个list中，但是所有的filters必须至少出现在其中一个list中
     */
    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        this.filters = filters;
        return Result.of(this.filters, Lists.newArrayList());
    }

}
