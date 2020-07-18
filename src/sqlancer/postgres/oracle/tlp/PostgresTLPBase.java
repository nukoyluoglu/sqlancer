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
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresSelect.ForClause;
import sqlancer.postgres.ast.PostgresSelect.PostgresFromTable;
import sqlancer.postgres.ast.PostgresSelect.PostgresCTE;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.gen.PostgresExpressionGenerator;
import sqlancer.postgres.oracle.PostgresNoRECOracle;
import sqlancer.postgres.ast.PostgresBinaryComparisonOperation;
import sqlancer.postgres.ast.PostgresBinaryLogicalOperation;
import sqlancer.postgres.ast.PostgresJoin.PostgresJoinType;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.ast.PostgresConstant.BooleanConstant;


public class PostgresTLPBase extends TernaryLogicPartitioningOracleBase<PostgresExpression, PostgresGlobalState>
        implements TestOracle {

    PostgresSchema s;
    PostgresTables targetTables;
    PostgresExpressionGenerator gen;
    PostgresSelect select;
    HashMap<PostgresTable, Integer> distributedTables;
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
        distributedTables = new HashMap<>();
        referenceTables = new ArrayList<>();
        localTables = new ArrayList<>();
        List<PostgresTable> allTables = s.getDatabaseTables();
        for (PostgresTable table : allTables) {
            Integer colocationId = table.getColocationId();
            PostgresColumn distributionColumn = table.getDistributionColumn();
            if (colocationId != null && distributionColumn != null) {
                distributedTables.put(table, colocationId);
            } else if (colocationId != null) {
                referenceTables.add(table);
            } else {
                localTables.add(table);
            }
        }
        List<PostgresJoin> joins = null;
        List<PostgresExpression> tableList = null;
        if (distributedTables.isEmpty() || (!referenceTables.isEmpty() && !Randomly.
            getBooleanWithRatherLowProbability())) {
            if (!localTables.isEmpty()) {
                // joins including only local tables
                // supports complex joins
                targetTables = new PostgresTables(Randomly.nonEmptySubset(localTables));
            }
            if (!referenceTables.isEmpty()) {
                // joins including reference tables
                // supports complex joins
                List<PostgresTable> targetTableList = new ArrayList<>(referenceTables);
                if (!distributedTables.isEmpty()) {
                    // joins including distributed and reference tables
                    // supports complex joins
                    targetTableList.add(Randomly.fromList(new ArrayList<>(distributedTables.keySet())));
                }
                targetTables = new PostgresTables(Randomly.nonEmptySubset(targetTableList));
            }
            List<PostgresTable> tables = new ArrayList<>(targetTables.getTables());
            joins = PostgresNoRECOracle.getJoinStatements(state, targetTables.getColumns(), tables);
            tableList = tables.stream().map(t -> new PostgresFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList());
        } else {
            // joins between distributed tables
            // join including distribution columns
            // supports complex joins if colocated
            List<PostgresTable> tables = Randomly.nonEmptySubset(new ArrayList<>(distributedTables.keySet()));
            targetTables = new PostgresTables(tables);
            PostgresTable fromTable = Randomly.fromList(tables);
            joins = getCitusJoinStatements(state, tables, fromTable);
            if (Randomly.getBooleanWithRatherLowProbability() && !localTables.isEmpty()) {
                PostgresJoin joinCTE = getCTEJoinStatement(state, localTables, fromTable);
                joins.add(joinCTE);
            }
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
        List<PostgresTable> joinTables, PostgresTable fromTable) {
        List<PostgresColumn> columns = new ArrayList<>();
        for (PostgresTable t : joinTables) {
            columns.add(t.getDistributionColumn());
        }
        List<PostgresJoin> joinStatements = new ArrayList<>();
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState).setColumns(columns);
        joinTables.remove(fromTable);
        boolean allColocated = true;
        for (PostgresTable t : joinTables) {
            boolean colocated = (distributedTables.get(fromTable) == distributedTables.get(t));
            allColocated = allColocated && colocated;
        }
        while (!joinTables.isEmpty()) {
            PostgresTable table = Randomly.fromList(joinTables);
            joinTables.remove(table);
            PostgresExpression joinClause = null;
            PostgresExpression equiJoinClause = null;
            if (allColocated) {
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
            if (allColocated && Randomly.getBooleanWithSmallProbability()) {
                joinClause = new PostgresBinaryLogicalOperation(equiJoinClause,
                gen.generateExpression(PostgresDataType.BOOLEAN), 
                PostgresBinaryLogicalOperation.BinaryLogicalOperator.AND);
            } else {
                joinClause = equiJoinClause;
            }
            PostgresJoinType options = Randomly.fromOptions(PostgresJoinType.INNER, PostgresJoinType.LEFT, PostgresJoinType.RIGHT, PostgresJoinType.FULL);
            if (!allColocated) {
                options = PostgresJoinType.INNER;
            }
            PostgresJoin j = new PostgresJoin(table, joinClause, options);
            joinStatements.add(j);
        }
        return joinStatements;
    }

    PostgresJoin getCTEJoinStatement(PostgresGlobalState globalState, List<PostgresTable> CTETables, PostgresTable fromTable) {
        PostgresTables tables = new PostgresTables(Randomly.nonEmptySubset(localTables));
        PostgresCTE CTE = createCTE(state, tables);
        List<PostgresColumn> columns = tables.getColumns();
        columns.addAll(fromTable.getColumns());
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState).setColumns(columns);
        PostgresExpression joinClause = gen.generateExpression(PostgresDataType.BOOLEAN);
        PostgresJoinType options = PostgresJoinType.getRandom();
        return new PostgresJoin(CTE, joinClause, options);
    }

    public void whereJoin() {
        List<PostgresJoin> joins = select.getJoinClauses();
        if (!joins.isEmpty()) {
            List<PostgresExpression> fromList = new ArrayList<>(select.getFromList());
            PostgresExpression whereClause = select.getWhereClause();
            if (whereClause == null) {
                whereClause = new BooleanConstant(true);
            }
            List<PostgresJoin> joinToWhere = Randomly.nonEmptySubset(joins);
            for (PostgresJoin j : joinToWhere) {
                joins.remove(j);
                if (j.joinCTE()) {
                    fromList.add(j.getCTE());
                } else {
                    fromList.add(new PostgresFromTable(j.getTable(), Randomly.getBoolean()));
                }
                whereClause = new PostgresBinaryLogicalOperation(j.getOnClause(), 
                    whereClause, PostgresBinaryLogicalOperation.BinaryLogicalOperator.AND);
            }
            select.setJoinClauses(joins);
            select.setFromList(fromList);
            select.setWhereClause(whereClause);
        }
/* 
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
            } */
            // select.setWhereClause(whereClause);
        // }
    }

    public static PostgresCTE createCTE(PostgresGlobalState globalState, PostgresTables tables) {
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState).setColumns(tables.getColumns());
        PostgresSelect selectCTE = new PostgresSelect();
        selectCTE.setFromList(tables.getTables().stream().map(t -> new PostgresFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList()));
        if (Randomly.getBoolean()) {
            selectCTE.setWhereClause(gen.generateExpression(0, PostgresDataType.BOOLEAN));
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            selectCTE.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
            if (Randomly.getBoolean()) {
                selectCTE.setHavingClause(gen.generateHavingClause());
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            selectCTE.setOrderByExpressions(gen.generateOrderBy());
        }
        if (Randomly.getBoolean()) {
            selectCTE.setLimitClause(PostgresConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                selectCTE.setOffsetClause(
                        PostgresConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            selectCTE.setForClause(ForClause.getRandom());
        }
        return new PostgresCTE(selectCTE, "cte");
    }

}
