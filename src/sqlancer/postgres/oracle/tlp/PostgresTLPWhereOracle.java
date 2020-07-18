package sqlancer.postgres.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresJoin;
import sqlancer.postgres.ast.PostgresExpression;

public class PostgresTLPWhereOracle extends PostgresTLPBase {

    public PostgresTLPWhereOracle(PostgresGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        // don't use immutable functions in SELECT queries
        state.setAllowedFunctionTypes(Arrays.asList('i'));
        super.check();
        boolean whereJoin = Randomly.getBooleanWithRatherLowProbability();
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        List<PostgresJoin> oldJoins = new ArrayList<>(select.getJoinClauses());
        List<PostgresExpression> oldFromList = new ArrayList<>(select.getFromList());
        if (whereJoin) {
            super.whereJoin(select);
        }
        state.setDefaultAllowedFunctionTypes();
        String originalQueryString = PostgresVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        select.setOrderByExpressions(Collections.emptyList());
        select.setJoinClauses(oldJoins);
        select.setFromList(oldFromList);
        oldJoins = new ArrayList<>(oldJoins);
        oldFromList = new ArrayList<>(oldFromList);
        select.setWhereClause(predicate);
        if (whereJoin) {
            super.whereJoin(select);
        }
        String firstQueryString = PostgresVisitor.asString(select);
        select.setJoinClauses(oldJoins);
        select.setFromList(oldFromList);
        oldJoins = new ArrayList<>(oldJoins);
        oldFromList = new ArrayList<>(oldFromList);
        select.setWhereClause(negatedPredicate);
        if (whereJoin) {
            super.whereJoin(select);
        }
        String secondQueryString = PostgresVisitor.asString(select);
        select.setJoinClauses(oldJoins);
        select.setFromList(oldFromList);
        select.setWhereClause(isNullPredicate);
        if (whereJoin) {
            super.whereJoin(select);
        }
        String thirdQueryString = PostgresVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }
}
