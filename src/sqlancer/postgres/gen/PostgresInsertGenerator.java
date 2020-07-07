package sqlancer.postgres.gen;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresExpression;

public final class PostgresInsertGenerator {

    private PostgresInsertGenerator() {
    }

    public static Query insert(PostgresGlobalState globalState) {
        PostgresTable table = globalState.getSchema().getRandomTable(t -> t.isInsertable());
        Set<String> errors = new HashSet<>();
        errors.add("cannot insert into column");
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonInsertUpdateErrors(errors);
        PostgresCommon.addCommonExpressionErrors(errors);
        // for queries not supported by Citus
        PostgresCommon.addCitusErrors(errors);
        errors.add("multiple assignments to same column");
        errors.add("violates foreign key constraint");
        errors.add("value too long for type character varying");
        errors.add("conflicting key value violates exclusion constraint");
        errors.add("violates not-null constraint");
        errors.add("current transaction is aborted");
        errors.add("bit string too long");
        errors.add("new row violates check option for view");
        errors.add("reached maximum value of sequence");
        errors.add("but expression is of type");
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(table.getName());
        List<PostgresColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("(");
        // INSERT must include partition column if table is distributed
        PostgresColumn distributionColumn = table.getDistributionColumn();
        // check if table is distributed
        if (distributionColumn != null) {
            // TODO: even if dist col has default value does not work
            boolean distributionColumnHasDefaultValue = table.getColumnsWithDefaultValues().stream().anyMatch(c -> c.getName().equals(distributionColumn.getName()));
            // check if distribution column does not have default value
            if (! distributionColumnHasDefaultValue) {
                boolean distributionColumnIncluded = columns.stream().anyMatch(c -> c.getName().equals(distributionColumn.getName()));
                // check if query does not include distribution column
                if (! distributionColumnIncluded) {
                    for (PostgresColumn c : table.getColumns()) {
                        if (c.getName().equals(distributionColumn.getName())) {
                            columns.add(c);
                       }
                    }
                }
            }
        }
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" OVERRIDING");
            sb.append(" ");
            sb.append(Randomly.fromOptions("SYSTEM", "USER"));
            sb.append(" VALUE");
        }
        sb.append(" VALUES");

        if (globalState.getDmbsSpecificOptions().allowBulkInsert && Randomly.getBooleanWithSmallProbability()) {
            StringBuilder sbRowValue = new StringBuilder();
            sbRowValue.append("(");
            for (int i = 0; i < columns.size(); i++) {
                if (i != 0) {
                    sbRowValue.append(", ");
                }
                // distribution column cannot contian null value
                // if (table.getDistributionColumn() != null && columns.get(i).getName().equals(table.getDistributionColumn().getName())) {
                //     // INSERT cannot be performed with NULL in the partition column on a distributed table
                //     String valueToInsert;
                //     do {
                //         valueToInsert = PostgresVisitor.asString(PostgresExpressionGenerator
                // .generateConstant(globalState.getRandomly(), columns.get(i).getType()));
                //     } while (valueToInsert.contains("NULL") || valueToInsert.contains("null"));
                //     sbRowValue.append(valueToInsert);
                // } else {
                    sbRowValue.append(PostgresVisitor.asString(PostgresExpressionGenerator
                        .generateConstant(globalState.getRandomly(), columns.get(i).getType())));
                // }
            }
            sbRowValue.append(")");

            int n = (int) Randomly.getNotCachedInteger(100, 1000);
            for (int i = 0; i < n; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(sbRowValue);
            }
        } else {
            int n = Randomly.smallNumber() + 1;
            for (int i = 0; i < n; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                insertRow(globalState, sb, columns, table, n == 1);
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" ON CONFLICT ");
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(table.getRandomColumn().getName());
                sb.append(")");
                errors.add("there is no unique or exclusion constraint matching the ON CONFLICT specification");
            }
            sb.append(" DO NOTHING");
        }
        errors.add("duplicate key value violates unique constraint");
        errors.add("identity column defined as GENERATED ALWAYS");
        errors.add("out of range");
        errors.add("violates check constraint");
        errors.add("no partition of relation");
        errors.add("invalid input syntax");
        errors.add("division by zero");
        errors.add("violates foreign key constraint");
        errors.add("data type unknown");
        return new QueryAdapter(sb.toString(), errors);
    }

    private static void insertRow(PostgresGlobalState globalState, StringBuilder sb, List<PostgresColumn> columns, PostgresTable table,
            boolean canBeDefault) {
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            if (!Randomly.getBooleanWithSmallProbability() || !canBeDefault) {
                PostgresExpression generateConstant;
                if (table.getDistributionColumn() != null && columns.get(i).getName().equals(table.getDistributionColumn().getName())) {
                    // INSERT cannot be performed with NULL in the partition column on a distributed table
                    String valueToInsert;
                    do {
                        if (Randomly.getBoolean()) {
                            generateConstant = PostgresExpressionGenerator.generateConstant(globalState.getRandomly(),
                                    columns.get(i).getType());
                        } else {
                            generateConstant = new PostgresExpressionGenerator(globalState)
                                    .generateExpression(columns.get(i).getType());
                        }
                        valueToInsert = PostgresVisitor.asString(generateConstant);
                    } while (valueToInsert.contains("NULL") || valueToInsert.contains("null"));
                    sb.append(valueToInsert);
                } else {
                    if (Randomly.getBoolean()) {
                        generateConstant = PostgresExpressionGenerator.generateConstant(globalState.getRandomly(),
                                columns.get(i).getType());
                    } else {
                        generateConstant = new PostgresExpressionGenerator(globalState)
                                .generateExpression(columns.get(i).getType());
                    }
                    sb.append(PostgresVisitor.asString(generateConstant));
                }    
            } else {
                sb.append("DEFAULT");
            }
        }
        sb.append(")");
    }

}
