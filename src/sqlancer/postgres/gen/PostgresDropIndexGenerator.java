package sqlancer.postgres.gen;

import java.util.Arrays;
import java.util.List;

import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.Randomly;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresIndex;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.postgres.PostgresSchema.PostgresTable;

public final class PostgresDropIndexGenerator {

    private PostgresDropIndexGenerator() {
    }

    public static Query create(PostgresGlobalState globalState) {
        PostgresTable randomTable = globalState.getSchema().getRandomTable();
        List<PostgresIndex> indexes = randomTable.getIndexes();
        StringBuilder sb = new StringBuilder();
        sb.append("DROP INDEX ");
        int numberofIndicesToDrop = Randomly.smallNumber() + 1;
        if (randomTable.getDistributionColumn() != null) {
            numberofIndicesToDrop = 1;
        }
        if (Randomly.getBoolean() || indexes.isEmpty()) {
            sb.append("IF EXISTS ");
            for (int i = 0; i < numberofIndicesToDrop; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                if (indexes.isEmpty() || Randomly.getBoolean()) {
                    sb.append(SQLite3Common.createIndexName(Randomly.smallNumber()));
                } else {
                    sb.append(Randomly.fromList(indexes).getIndexName());
                }
            }
        } else {
            for (int i = 0; i < numberofIndicesToDrop; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(Randomly.fromList(indexes).getIndexName());
            }
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("CASCADE", "RESTRICT"));
        }
        return new QueryAdapter(sb.toString(),
                Arrays.asList("cannot drop desired object(s) because other objects depend on them", "cannot drop index",
                        "does not exist"),
                true);
    }

}
