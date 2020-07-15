package sqlancer.postgres.oracle.tlp;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.ArrayList;

import sqlancer.Randomly;
import sqlancer.TernaryLogicPartitioningOracleBase;
import sqlancer.TestOracle;
import sqlancer.gen.ExpressionGenerator;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.ast.PostgresPostfixOperation;
import sqlancer.postgres.ast.PostgresPostfixOperation.PostfixOperator;
import sqlancer.postgres.ast.PostgresPrefixOperation;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.ForClause;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;
import sqlancer.postgres.oracle.PostgresNoRECOracle;
import sqlancer.postgres.ast.PostgresBinaryComparisonOperation;
import sqlancer.postgres.ast.PostgresBinaryLogicalOperation;
import sqlancer.postgres.ast.PostgresJoin.PostgresJoinType;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.ast.PostgresConstant.BooleanConstant;


public class PostgresTLPBase extends TernaryLogicPartitioningOracleBase<PostgresExpression, PostgresGlobalState>
        implements TestOracle {

    PostgresSchema s;
    PostgresTables targetTables;
    PostgresExpressionGenerator gen;
    PostgresSelect select;
    HashMap<Integer, List<PostgresTable>> distributedColocationGroups;
    List<PostgresTable> referenceTables;
    List<PostgresTable> localTables;

    public PostgresTLPBase(PostgresGlobalState state) {
        super(state);
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonFetchErrors(errors);
        // for queries not supported by Citus
        PostgresCommon.addCitusErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        // clear left-over query string from previous test
        state.getState().queryString = null;
        s = state.getSchema();
        distributedColocationGroups = new HashMap<>();
        referenceTables = new ArrayList<>();
        localTables = new ArrayList<>();
        List<PostgresTable> allTables = s.getDatabaseTables();
        for (PostgresTable table : allTables) {
            Integer colocationId = table.getColocationId();
            PostgresColumn distributionColumn = table.getDistributionColumn();
            if (colocationId != null && distributionColumn != null) {
                if (distributedColocationGroups.containsKey(colocationId)) {
                    distributedColocationGroups.get(colocationId).add(table);
                } else {
                    distributedColocationGroups.put(colocationId, new ArrayList<>(Arrays.asList(table)));
                }
            } else if (colocationId != null) {
                referenceTables.add(table);
            } else {
                localTables.add(table);
            }
        }
        List<PostgresJoin> joins = null;
        List<PostgresExpression> tableList = null;
        if (distributedColocationGroups.isEmpty() || (!referenceTables.isEmpty() && !Randomly.getBooleanWithRatherLowProbability())) {
            if (!localTables.isEmpty()) {
                // joins including only local tables
                // supports complex joins
                targetTables = new PostgresTables(Randomly.nonEmptySubset(localTables));
            }
            if (!referenceTables.isEmpty()) {
                // joins including reference tables
                // supports complex joins
                List<PostgresTable> targetTableList = new ArrayList<>(referenceTables);
                if (!distributedColocationGroups.isEmpty()) {
                    // joins including distributed and reference tables
                    // supports complex joins
                    for (int colId : distributedColocationGroups.keySet()) {
                        targetTableList.addAll(distributedColocationGroups.get(colId));
                    }
                }
                targetTables = new PostgresTables(Randomly.nonEmptySubset(targetTableList));
            }
            List<PostgresTable> tables = new ArrayList<>(targetTables.getTables());
            joins = PostgresNoRECOracle.getJoinStatements(state, targetTables.getColumns(), tables);
            tableList = tables.stream().map(t -> new PostgresFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList());
        } else {
            // joins between distributed tables not necessarily colocated
            // join including distribution columns
            // does not support complex joins
            List<Integer> colocationIds = Randomly.nonEmptySubset(new ArrayList<>(distributedColocationGroups.keySet()));
            List<PostgresTable> tables = new ArrayList<>();
            for (int colId : colocationIds) {
                tables.addAll(distributedColocationGroups.get(colId));
            }
            tables = Randomly.nonEmptySubset(tables);
            targetTables = new PostgresTables(tables);
            PostgresTable fromTable = Randomly.fromList(tables);
            boolean colocated = false;
            if (colocationIds.size() == 1) {
                // joins between colocated distributed tables
                // equijoin on distribution columns
                // supports complex joins
                colocated = true;
            }
            joins = getCitusJoinStatements(state, tables, fromTable, colocated);
            tableList = Arrays.asList(new PostgresFromTable(fromTable, Randomly.getBoolean()));
        }
        // TODO joins
        gen = new PostgresExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new PostgresSelect();
        select.setFetchColumns(generateFetchColumns());
        select.setFromList(tableList);
        select.setWhereClause(null);
        select.setJoinClauses(joins);
        if (Randomly.getBoolean()) {
            select.setForClause(ForClause.getRandom());
        }
    }

    List<PostgresExpression> generateFetchColumns() {
        List<PostgresExpression> fetchColumns = new ArrayList<>();
        List<PostgresColumn> targetColumns = Randomly.nonEmptySubset(targetTables.getColumns());
        for (PostgresColumn c : targetColumns) {
            fetchColumns.add(new PostgresColumnValue(c, null));
        }
        return fetchColumns;
    }

    @Override
    protected ExpressionGenerator<PostgresExpression> getGen() {
        return gen;
    }

    List<PostgresJoin> getCitusJoinStatements(PostgresGlobalState globalState, 
        List<PostgresTable> joinTables, PostgresTable fromTable, boolean colocated) {
        List<PostgresColumn> columns = new ArrayList<>();
        for (PostgresTable t : joinTables) {
            columns.add(t.getDistributionColumn());
        }
        List<PostgresJoin> joinStatements = new ArrayList<>();
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState).setColumns(columns);
        joinTables.remove(fromTable);
        while (!joinTables.isEmpty()) {
            PostgresTable table = Randomly.fromList(joinTables);
            joinTables.remove(table);
            PostgresExpression joinClause = null;
            PostgresExpression equiJoinClause = null;
            if (colocated) {
                PostgresExpression leftExpr = new PostgresColumnValue(fromTable.getDistributionColumn(), null);
                PostgresExpression rightExpr = new PostgresColumnValue(table.getDistributionColumn(), null);
                equiJoinClause = new PostgresBinaryComparisonOperation(leftExpr, rightExpr,
                    PostgresBinaryComparisonOperation.PostgresBinaryComparisonOperator.EQUALS);
            } else {
                PostgresExpression leftExpr = new PostgresColumnValue(fromTable.getDistributionColumn(), null);
                List<PostgresColumn> candidateRightColumns = table.getColumns().stream().filter(c -> c.getType().equals(fromTable.getDistributionColumn().getType())).collect(Collectors.toList());
                if (candidateRightColumns.isEmpty()) {
                    continue;
                }
                PostgresExpression rightExpr = new PostgresColumnValue(Randomly.fromList(candidateRightColumns), null);
                equiJoinClause = new PostgresBinaryComparisonOperation
                    (leftExpr, rightExpr, PostgresBinaryComparisonOperation.PostgresBinaryComparisonOperator.EQUALS);
            }
            if (Randomly.getBooleanWithSmallProbability()) {
                joinClause = new PostgresBinaryLogicalOperation(equiJoinClause,
                gen.generateExpression(PostgresDataType.BOOLEAN), 
                PostgresBinaryLogicalOperation.BinaryLogicalOperator.AND);
            } else {
                joinClause = equiJoinClause;
            }
            PostgresJoinType options = Randomly.fromOptions(PostgresJoinType.INNER, PostgresJoinType.LEFT, PostgresJoinType.RIGHT, PostgresJoinType.FULL);
            if (!colocated) {
                options = PostgresJoinType.INNER;
            }
            PostgresJoin j = new PostgresJoin(table, joinClause, options);
            joinStatements.add(j);
        }
        return joinStatements;
    }

    public void whereJoin() {
        select.setJoinClauses(new ArrayList<>());
        List<PostgresTable> tables = targetTables.getTables();
        List<PostgresExpression> tableList = tables.stream().map(t -> new PostgresFromTable(t, Randomly.getBoolean()))
            .collect(Collectors.toList());
        select.setFromList(tableList);
        if (tables.size() > 1) {
            PostgresExpression whereClause = select.getWhereClause();
            if (whereClause == null) {
                whereClause = new BooleanConstant(true);
            }
            PostgresExpression leftExpr = new PostgresColumnValue(tables.get(0).getDistributionColumn(), null);
            for (int i = 1; i < tables.size(); i++) {
                PostgresExpression rightExpr = new PostgresColumnValue(tables.get(i).getDistributionColumn(), null);
                PostgresExpression equiWhereClause = new PostgresBinaryComparisonOperation(leftExpr, rightExpr,
                    PostgresBinaryComparisonOperation.PostgresBinaryComparisonOperator.EQUALS);
                whereClause = new PostgresBinaryLogicalOperation(equiWhereClause, 
                    whereClause, PostgresBinaryLogicalOperation.BinaryLogicalOperator.AND);
            }
            select.setWhereClause(whereClause);
        }
    }

}
