package sqlancer.postgres;

import java.io.FileWriter;
import java.sql.ResultSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.HashSet;

import sqlancer.AbstractAction;
import sqlancer.CompositeTestOracle;
import sqlancer.IgnoreMeException;
import sqlancer.ProviderAdapter;
import sqlancer.Query;
import sqlancer.QueryAdapter;
import sqlancer.QueryProvider;
import sqlancer.Randomly;
import sqlancer.StateToReproduce;
import sqlancer.StateToReproduce.PostgresStateToReproduce;
import sqlancer.StatementExecutor;
import sqlancer.TestOracle;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.gen.PostgresAlterTableGenerator;
import sqlancer.postgres.gen.PostgresAnalyzeGenerator;
import sqlancer.postgres.gen.PostgresClusterGenerator;
import sqlancer.postgres.gen.PostgresCommentGenerator;
import sqlancer.postgres.gen.PostgresDeleteGenerator;
import sqlancer.postgres.gen.PostgresDiscardGenerator;
import sqlancer.postgres.gen.PostgresDropIndexGenerator;
import sqlancer.postgres.gen.PostgresIndexGenerator;
import sqlancer.postgres.gen.PostgresInsertGenerator;
import sqlancer.postgres.gen.PostgresNotifyGenerator;
import sqlancer.postgres.gen.PostgresQueryCatalogGenerator;
import sqlancer.postgres.gen.PostgresReindexGenerator;
import sqlancer.postgres.gen.PostgresSequenceGenerator;
import sqlancer.postgres.gen.PostgresSetGenerator;
import sqlancer.postgres.gen.PostgresStatisticsGenerator;
import sqlancer.postgres.gen.PostgresTableGenerator;
import sqlancer.postgres.gen.PostgresTransactionGenerator;
import sqlancer.postgres.gen.PostgresTruncateGenerator;
import sqlancer.postgres.gen.PostgresUpdateGenerator;
import sqlancer.postgres.gen.PostgresVacuumGenerator;
import sqlancer.postgres.gen.PostgresViewGenerator;
import sqlancer.sqlite3.gen.SQLite3Common;
import static sqlancer.postgres.PostgresSchema.getColumnType;

// EXISTS
// IN
public final class PostgresProvider extends ProviderAdapter<PostgresGlobalState, PostgresOptions> {

    public static boolean generateOnlyKnown;

    private PostgresGlobalState globalState;

    public PostgresProvider() {
        super(PostgresGlobalState.class, PostgresOptions.class);
    }

    public enum Action implements AbstractAction<PostgresGlobalState> {
        ANALYZE(PostgresAnalyzeGenerator::create), //
        ALTER_TABLE(g -> PostgresAlterTableGenerator.create(g.getSchema().getRandomTable(t -> !t.isView()), g,
                generateOnlyKnown)), //
        CLUSTER(PostgresClusterGenerator::create), //
        COMMIT(g -> {
            Query query;
            if (Randomly.getBoolean()) {
                query = new QueryAdapter("COMMIT", true);
            } else if (Randomly.getBoolean()) {
                query = PostgresTransactionGenerator.executeBegin();
            } else {
                query = new QueryAdapter("ROLLBACK", true);
            }
            return query;
        }), //
        CREATE_STATISTICS(PostgresStatisticsGenerator::insert), //
        DROP_STATISTICS(PostgresStatisticsGenerator::remove), //
        DELETE(PostgresDeleteGenerator::create), //
        DISCARD(PostgresDiscardGenerator::create), //
        DROP_INDEX(PostgresDropIndexGenerator::create), //
        INSERT(PostgresInsertGenerator::insert), //
        UPDATE(PostgresUpdateGenerator::create), //
        TRUNCATE(PostgresTruncateGenerator::create), //
        VACUUM(PostgresVacuumGenerator::create), //
        REINDEX(PostgresReindexGenerator::create), //
        SET(PostgresSetGenerator::create), //
        CREATE_INDEX(PostgresIndexGenerator::generate), //
        SET_CONSTRAINTS((g) -> {
            StringBuilder sb = new StringBuilder();
            sb.append("SET CONSTRAINTS ALL ");
            sb.append(Randomly.fromOptions("DEFERRED", "IMMEDIATE"));
            return new QueryAdapter(sb.toString());
        }), //
        RESET_ROLE((g) -> new QueryAdapter("RESET ROLE")), //
        COMMENT_ON(PostgresCommentGenerator::generate), //
        RESET((g) -> new QueryAdapter("RESET ALL") /*
                                                    * https://www.postgresql.org/docs/devel/sql-reset.html TODO: also
                                                    * configuration parameter
                                                    */), //
        NOTIFY(PostgresNotifyGenerator::createNotify), //
        LISTEN((g) -> PostgresNotifyGenerator.createListen()), //
        UNLISTEN((g) -> PostgresNotifyGenerator.createUnlisten()), //
        CREATE_SEQUENCE(PostgresSequenceGenerator::createSequence), //
        CREATE_VIEW(PostgresViewGenerator::create), //
        QUERY_CATALOG((g) -> PostgresQueryCatalogGenerator.query());

        private final QueryProvider<PostgresGlobalState> queryProvider;

        Action(QueryProvider<PostgresGlobalState> queryProvider) {
            this.queryProvider = queryProvider;
        }

        @Override
        public Query getQuery(PostgresGlobalState state) throws SQLException {
            return queryProvider.getQuery(state);
        }
    }

    private static int mapActions(PostgresGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed;
        switch (a) {
        case CREATE_INDEX:
        case CLUSTER:
            nrPerformed = r.getInteger(0, 3);
            break;
        case CREATE_STATISTICS:
            nrPerformed = r.getInteger(0, 5);
            break;
        case DISCARD:
        case DROP_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case COMMIT:
            nrPerformed = r.getInteger(0, 0);
            break;
        case ALTER_TABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case REINDEX:
        case RESET:
            nrPerformed = r.getInteger(0, 3);
            break;
        case DELETE:
        case RESET_ROLE:
        case SET:
        case QUERY_CATALOG:
            nrPerformed = r.getInteger(0, 5);
            break;
        case ANALYZE:
            nrPerformed = r.getInteger(0, 3);
            break;
        case VACUUM:
        case SET_CONSTRAINTS:
        case COMMENT_ON:
        case NOTIFY:
        case LISTEN:
        case UNLISTEN:
        case CREATE_SEQUENCE:
        case DROP_STATISTICS:
        case TRUNCATE:
            nrPerformed = r.getInteger(0, 2);
            break;
        case CREATE_VIEW:
            nrPerformed = r.getInteger(0, 2);
            break;
        case UPDATE:
            nrPerformed = r.getInteger(0, 10);
            break;
        case INSERT:
            nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            break;
        default:
            throw new AssertionError(a);
        }
        return nrPerformed;

    }

    // FIXME: static or not?
    private class WorkerNode{

        private final String name;
        private final int port;

        public WorkerNode(String node_name, int node_port) {
            this.name = node_name;
            this.port = node_port; 
        }

        public String get_name() {
            return this.name;
        }

        public int get_port() {
            return this.port;
        }

    }

    // FIXME: static or not?
    private final void distributeTable(List<PostgresColumn> columns, String tableName, PostgresGlobalState globalState, Connection con) throws SQLException {
        if (columns.size() != 0) {
            PostgresColumn columnToDistribute = Randomly.fromList(columns);
            QueryAdapter query = new QueryAdapter("SELECT create_distributed_table('" + tableName + "', '" + columnToDistribute.getName() + "');");
            globalState.getState().statements.add(query);
            String template = "SELECT create_distributed_table(?, ?);";
            List<String> fills = Arrays.asList(tableName, columnToDistribute.getName());
            query.fillAndExecute(con, template, fills);
            // distribution column cannot take NULL value
            // TODO: find a way to protect from SQL injection without '' around string input
            query = new QueryAdapter("ALTER TABLE " + tableName + " ALTER COLUMN " + columnToDistribute.getName() + " SET NOT NULL;");
            globalState.getState().statements.add(query);
            query.execute(con);
        }
    }

    private final List<String> getTableConstraints(String tableName, PostgresGlobalState globalState, Connection con) throws SQLException {
        List<String> constraints = new ArrayList<>();
        QueryAdapter query = new QueryAdapter("SELECT constraint_type FROM information_schema.table_constraints WHERE table_name = '" + tableName + "' AND (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' or constraint_type = 'EXCLUDE');");
        // TODO: decide whether to log
        // globalState.getState().statements.add(query);
        String template = "SELECT constraint_type FROM information_schema.table_constraints WHERE table_name = ? AND (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' or constraint_type = 'EXCLUDE');";
        List<String> fills = new ArrayList<>();
        fills.add(tableName);
        ResultSet rs = query.fillAndExecuteAndGet(con, template, fills);
        while (rs.next()) {
            constraints.add(rs.getString("constraint_type"));
        }
        return constraints;
    }

    // FIXME: static or not?
    private final void createDistributedTable(String tableName, PostgresGlobalState globalState, Connection con) throws SQLException {
        List<PostgresColumn> columns = new ArrayList<>();
        List<String> tableConstraints = getTableConstraints(tableName, globalState, con);
        if (tableConstraints.size() == 0) {
            QueryAdapter query = new QueryAdapter("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '" + tableName + "';");
            // TODO: decide whether to log
            // globalState.getState().statements.add(query);
            String template = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?;";
            List<String> fills = Arrays.asList(tableName);
            ResultSet rs = query.fillAndExecuteAndGet(con, template, fills);
            while (rs.next()) {
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                // data types money & bit varying have no default operator class for specified partition method
                if (! (dataType.equals("money") || dataType.equals("bit varying"))) {
                    PostgresColumn c = new PostgresColumn(columnName, getColumnType(dataType));
                    columns.add(c);
                }
            }
        } else {
            // TODO: multiple constraints?
            HashMap<PostgresColumn, List<String>> columnConstraints = new HashMap<>();
            QueryAdapter query = new QueryAdapter("SELECT c.column_name, c.data_type, tc.constraint_type FROM information_schema.table_constraints tc JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name WHERE (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' OR constraint_type = 'EXCLUDE') AND c.table_name = '" + tableName + "';");
            // TODO: decide whether to log
            // globalState.getState().statements.add(query);
            String template = "SELECT c.column_name, c.data_type, tc.constraint_type FROM information_schema.table_constraints tc JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name WHERE (constraint_type = 'PRIMARY KEY' OR constraint_type = 'UNIQUE' OR constraint_type = 'EXCLUDE') AND c.table_name = ?;";
            List<String> fills = Arrays.asList(tableName);
            ResultSet rs = query.fillAndExecuteAndGet(con, template, fills);
            while (rs.next()) {
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                String constraintType = rs.getString("constraint_type");
                // data types money & bit varying have no default operator class for specified partition method
                if (! (dataType.equals("money") || dataType.equals("bit varying"))) {
                    PostgresColumn c = new PostgresColumn(columnName, getColumnType(dataType));
                    if (columnConstraints.containsKey(c)) {
                        columnConstraints.get(c).add(constraintType);
                    } else {
                        columnConstraints.put(c, Arrays.asList(constraintType));
                    }
                }  
            }
            for (PostgresColumn c : columnConstraints.keySet()) {
                // TODO: check if table and column constraint sets are equal? but then it's O(N) instead of O(1)
                if (tableConstraints.size() == columnConstraints.get(c).size()) {
                    columns.add(c);
                }
            }
            // TODO: figure out how to use EXCLUDE
        }
        distributeTable(columns, tableName, globalState, con);
    }

    @Override
    public void generateDatabase(PostgresGlobalState globalState) throws SQLException {
        QueryAdapter query = new QueryAdapter("SELECT proname, provolatile FROM pg_proc;");
        globalState.getState().statements.add(query);
        ResultSet rs = query.executeAndGet(con);
        while (rs.next()) {
            String functionName = rs.getString("proname");
            Character functionType = rs.getString("provolatile").charAt(0);
            globalState.addFunctionAndType(functionName, functionType);
        }

        while (globalState.getSchema().getDatabaseTables().size() < Randomly.fromOptions(4, 5, 6)) {
            try {
                String tableName = SQLite3Common.createTableName(globalState.getSchema().getDatabaseTables().size());
                Query createTable = PostgresTableGenerator.generate(tableName, globalState.getSchema(),
                        generateOnlyKnown, globalState);
                globalState.executeStatement(createTable);
                }
                globalState.setSchema(PostgresSchema.fromConnection(con, databaseName));
            } catch (IgnoreMeException e) {

            }
        }

        for (PostgresTable table : globalState.getSchema().getDatabaseTables()) {
            // TODO: random 0-1 range double
            if (Randomly.getBooleanWithRatherLowProbability()) {
                // create local table
            } else if (Randomly.getBooleanWithRatherLowProbability()) {
                // create reference table
                query = new QueryAdapter("SELECT create_reference_table('" + table.getName() + "');");
                globalState.getState().statements.add(query);
                String template = "SELECT create_reference_table(?);";
                List<String> fills = Arrays.asList(table.getName());
                // TODO: get rid of con dependence
                query.fillAndExecute(globalState.getConnection(), template, fills);
            } else {
                // create distributed table
                createDistributedTable(table.getName(), globalState, globalState.getConnection());
            }
        }
        // globalState.setSchema(PostgresSchema.fromConnection(globalState.getConnection(), globalState.getDatabaseName());
        globalState.updateSchema();

        StatementExecutor<PostgresGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                PostgresProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        // TODO: transactions broke during refactoring
        // catch (Throwable t) {
        // if (t.getMessage().contains("current transaction is aborted")) {
        // manager.execute(new QueryAdapter("ABORT"));
        // globalState.setSchema(PostgresSchema.fromConnection(con, databaseName));
        // } else {
        // System.err.println(query.getQueryString());
        // throw t;
        // }
        // }
        se.executeStatements();
        globalState.executeStatement(new QueryAdapter("COMMIT", true));
        globalState.executeStatement(new QueryAdapter("SET SESSION statement_timeout = 5000;\n"));
    }

    @Override
    protected TestOracle getTestOracle(PostgresGlobalState globalState) throws SQLException {
        List<TestOracle> oracles = globalState.getDmbsSpecificOptions().oracle.stream().map(o -> {
            try {
                return o.create(globalState);
            } catch (SQLException e1) {
                throw new AssertionError(e1);
            }
        }).collect(Collectors.toList());
        return new CompositeTestOracle(oracles);
    }
    
    @Override
    public Connection createDatabase(PostgresGlobalState globalState) throws SQLException {
        // lock database creation process per thread
        synchronized(PostgresProvider.class) {
            String databaseName = globalState.getDatabaseName();
            String username = globalState.getOptions().getUserName();
            String password = globalState.getOptions().getPassword();
            int coordinatorPort = ((PostgresGlobalState)globalState).getDmbsSpecificOptions().coordinatorPort;
            String entryDatabaseName = ((PostgresGlobalState)globalState).getDmbsSpecificOptions().entryDatabaseName;
            String urlCoordinatorInitDB = "jdbc:postgresql://localhost:" + coordinatorPort + "/" + entryDatabaseName;
            globalState.getState().statements.add(new QueryAdapter("psql -p " + coordinatorPort));
            globalState.getState().statements.add(new QueryAdapter("\\c " + entryDatabaseName));
            Connection con = DriverManager.getConnection(urlCoordinatorInitDB, username, password);
            globalState.getState().statements.add(new QueryAdapter("SELECT * FROM master_get_active_worker_nodes()"));
            globalState.getState().statements.add(new QueryAdapter("DROP DATABASE IF EXISTS " + databaseName));
            String createDatabaseCommand = getCreateDatabaseCommand(databaseName, con, globalState);
            globalState.getState().statements.add(new QueryAdapter(createDatabaseCommand));
            List<WorkerNode> workerNodes = new ArrayList<>();
            // get info about all servers hosting worker nodes
            try (Statement s = con.createStatement()) {
                ResultSet rs = s.executeQuery("SELECT * FROM master_get_active_worker_nodes();");
                while (rs.next()) {
                    String node_name = rs.getString("node_name");
                    int node_port = rs.getInt("node_port");
                    WorkerNode w = new WorkerNode(node_name, node_port);
                    workerNodes.add(w);
                }
            }
            try (Statement s = con.createStatement()) {
                s.execute("DROP DATABASE IF EXISTS " + databaseName);
            }
            try (Statement s = con.createStatement()) {
                s.execute(createDatabaseCommand);
            }
            con.close();
            for (WorkerNode w : workerNodes) {
                // create database with given databaseName at each server hosting worker node
                String urlWorkerInitDB = "jdbc:postgresql://localhost:" + w.get_port() + "/" + entryDatabaseName;
                globalState.getState().statements.add(new QueryAdapter("\\q"));
                globalState.getState().statements.add(new QueryAdapter("psql -p " + w.get_port()));
                globalState.getState().statements.add(new QueryAdapter("\\c " + entryDatabaseName));
                con = DriverManager.getConnection(urlWorkerInitDB, username, password);
                globalState.getState().statements.add(new QueryAdapter("DROP DATABASE IF EXISTS " + databaseName));
                createDatabaseCommand = getCreateDatabaseCommand(databaseName, con);
                globalState.getState().statements.add(new QueryAdapter(createDatabaseCommand));
                try (Statement s = con.createStatement()) {
                    s.execute("DROP DATABASE IF EXISTS " + databaseName);
                }
                try (Statement s = con.createStatement()) {
                    s.execute(createDatabaseCommand);
                }
                con.close();
                // add citus extension to database with given databaseName at each server hosting worker nodes
                String urlWorkerCurDB = "jdbc:postgresql://localhost:" + w.get_port() + "/" + databaseName;
                globalState.getState().statements.add(new QueryAdapter("\\c " + databaseName));
                con = DriverManager.getConnection(urlWorkerCurDB, username, password);
                globalState.getState().statements.add(new QueryAdapter("CREATE EXTENSION citus;"));
                try (Statement s = con.createStatement()) {
                    s.execute("CREATE EXTENSION citus;");
                }
                con.close();
            }
            globalState.getState().statements.add(new QueryAdapter("\\q"));
            globalState.getState().statements.add(new QueryAdapter("psql -p " + coordinatorPort));
            globalState.getState().statements.add(new QueryAdapter("\\c " + databaseName));
            globalState.getState().statements.add(new QueryAdapter("CREATE EXTENSION citus;"));
            String urlCoordinatorCurDB = "jdbc:postgresql://localhost:" + coordinatorPort + "/" + databaseName;
            con = DriverManager.getConnection(urlCoordinatorCurDB, username, password);
            // add citus extension to database with given databaseName at server hosting coordinator node
            try (Statement s = con.createStatement()) {
                s.execute("CREATE EXTENSION citus;");
            }
            // add all servers hosting worker nodes as worker nodes to coordinator node for database with given databaseName
            for (WorkerNode w : workerNodes) {
                // TODO: protect from sql injection - is it necessary though since these are read from the system?
                globalState.getState().statements.add(new QueryAdapter("SELECT * from master_add_node('" + w.get_name() + "', " + w.get_port() + ");"));
                String template = "SELECT * from master_add_node('" + w.get_name() + "', " + w.get_port() + ");";
                try (Statement s = con.createStatement()) {
                    s.execute("SELECT * from master_add_node('" + w.get_name() + "', " + w.get_port() + ");");
                }
            }
            List<String> statements = Arrays.asList(
                    // "CREATE EXTENSION IF NOT EXISTS btree_gin;",
                    // "CREATE EXTENSION IF NOT EXISTS btree_gist;", // TODO: undefined symbol: elog_start
                    "CREATE EXTENSION IF NOT EXISTS pg_prewarm;", "SET max_parallel_workers_per_gather=" + ((PostgresGlobalState)globalState).getDmbsSpecificOptions().max_parallel_workers_per_gather);
            for (String s : statements) {
                QueryAdapter query = new QueryAdapter(s);
                globalState.getState().statements.add(query);
                query.execute(con);
            }
            // new QueryAdapter("set jit_above_cost = 0; set jit_inline_above_cost = 0; set jit_optimize_above_cost =
            // 0;").execute(con);
            return con;
        }
    }

    private String getCreateDatabaseCommand(String databaseName, Connection con, PostgresGlobalState state) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DATABASE " + databaseName + " ");
        if (Randomly.getBoolean() && ((PostgresOptions) state.getDmbsSpecificOptions()).testCollations) {
            if (Randomly.getBoolean()) {
                sb.append("WITH ENCODING '");
                sb.append(Randomly.fromOptions("utf8"));
                sb.append("' ");
            }
            for (String lc : Arrays.asList("LC_COLLATE", "LC_CTYPE")) {
                if (Randomly.getBoolean()) {
                    globalState = new PostgresGlobalState();
                    globalState.setConnection(con);
                    sb.append(String.format(" %s = '%s'", lc, Randomly.fromList(globalState.getCollates())));
                }
            }
            sb.append(" TEMPLATE template0");
        }
        return sb.toString();
    }

    @Override
    public String getDBMSName() {
        return "postgres";
    }

    @Override
    public void printDatabaseSpecificState(FileWriter writer, StateToReproduce state) {
        StringBuilder sb = new StringBuilder();
        PostgresStateToReproduce specificState = (PostgresStateToReproduce) state;
        if (specificState.getRandomRowValues() != null) {
            List<PostgresColumn> columnList = specificState.getRandomRowValues().keySet().stream()
                    .collect(Collectors.toList());
            List<PostgresTable> tableList = columnList.stream().map(c -> c.getTable()).distinct().sorted()
                    .collect(Collectors.toList());
            for (PostgresTable t : tableList) {
                sb.append("-- " + t.getName() + "\n");
                List<PostgresColumn> columnsForTable = columnList.stream().filter(c -> c.getTable().equals(t))
                        .collect(Collectors.toList());
                for (PostgresColumn c : columnsForTable) {
                    sb.append("--\t");
                    sb.append(c);
                    sb.append("=");
                    sb.append(specificState.getRandomRowValues().get(c));
                    sb.append("\n");
                }
            }
            sb.append("expected values: \n");
            PostgresExpression whereClause = ((PostgresStateToReproduce) state).getWhereClause();
            if (whereClause != null) {
                sb.append(PostgresVisitor.asExpectedValues(whereClause).replace("\n", "\n-- "));
            }
        }
        try {
            writer.write(sb.toString());
            writer.flush();
        } catch (IOException e) {
            throw new AssertionError();
        }
    }

    @Override
    public StateToReproduce getStateToReproduce(String databaseName) {
        return new PostgresStateToReproduce(databaseName);
    }

}
