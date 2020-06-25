package sqlancer.postgres.gen;

import java.util.HashSet;
import java.util.Set;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;

public final class PostgresDeleteGenerator {

    private PostgresDeleteGenerator() {
    }

    public static Query create(PostgresGlobalState globalState) {
        PostgresTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        Set<String> errors = new HashSet<>();
        errors.add("violates foreign key constraint");
        errors.add("violates not-null constraint");
        errors.add("could not determine which collation to use for string comparison");
        StringBuilder sb = new StringBuilder("DELETE FROM");
        if (Randomly.getBoolean()) {
            sb.append(" ONLY");
        }
        sb.append(" ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(PostgresVisitor.asString(PostgresExpressionGenerator.generateExpression(globalState,
                    table.getColumns(), PostgresDataType.BOOLEAN)));
        }
        if (Randomly.getBoolean()) {
            sb.append(" RETURNING ");
            // non-IMMUTABLE functions cannot be used in RETURNING clauses on distributed tables
            globalState.setAllowStableFunction(false);
            globalState.setAllowVolatileFunction(false);
            sb.append(PostgresVisitor
                    .asString(PostgresExpressionGenerator.generateExpression(globalState, table.getColumns())));
            globalState.setAllowStableFunction(true);
            globalState.setAllowVolatileFunction(true);
        }
        PostgresCommon.addCommonExpressionErrors(errors);
        errors.add("out of range");
        errors.add("cannot cast");
        errors.add("invalid input syntax for");
        errors.add("division by zero");
        return new QueryAdapter(sb.toString(), errors);
    }

}
