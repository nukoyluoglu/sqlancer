package sqlancer.postgres.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.gen.PostgresCommon;

public class PostgresTLPHavingOracle extends PostgresTLPBase {

    public PostgresTLPHavingOracle(PostgresGlobalState state) {
        super(state);
        PostgresCommon.addGroupingErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        // don't use immutable functions in SELECT queries
        state.setAllowedFunctionTypes(Arrays.asList('i'));
        super.check();
        // TODO: add WHERE clause for dist column hardcoding
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(PostgresDataType.BOOLEAN)); 
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            super.whereJoin();
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);
        String originalQueryString = PostgresVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        state.setDefaultAllowedFunctionTypes();
        select.setHavingClause(predicate);
        String firstQueryString = PostgresVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = PostgresVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = PostgresVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    protected PostgresExpression generatePredicate() {
        return gen.generateHavingClause();
    }

    @Override
    List<PostgresExpression> generateFetchColumns() {
        List<PostgresExpression> expressions = gen.allowAggregates(true)
                .generateExpressions(Randomly.smallNumber() + 1);
        gen.allowAggregates(false);
        return expressions;
    }

}
