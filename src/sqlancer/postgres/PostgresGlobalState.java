package sqlancer.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;

import sqlancer.GlobalState;
import sqlancer.Randomly;

public class PostgresGlobalState extends GlobalState<PostgresOptions, PostgresSchema> {

    private List<String> operators;
    private List<String> collates;
    private List<String> opClasses;

    private HashSet<String> volatileFunctions;
    private HashSet<String> stableFunctions;
    private HashSet<String> immutableFunctions;
    private boolean allowVolatileFunction = true;
    private boolean allowStableFunction = true;
    private boolean allowImmutableFunction = true;
>>>>>>> fix citus-specific query errors

    @Override
    public void setConnection(Connection con) {
        super.setConnection(con);
        try {
            this.opClasses = getOpclasses(getConnection());
            this.operators = getOperators(getConnection());
            this.collates = getCollnames(getConnection());
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    private List<String> getCollnames(Connection con) throws SQLException {
        List<String> opClasses = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery("SELECT collname FROM pg_collation WHERE collname LIKE '%utf8' or collname = 'C';")) {
                while (rs.next()) {
                    opClasses.add(rs.getString(1));
                }
            }
        }
        return opClasses;
    }

    private List<String> getOpclasses(Connection con) throws SQLException {
        List<String> opClasses = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select opcname FROM pg_opclass;")) {
                while (rs.next()) {
                    opClasses.add(rs.getString(1));
                }
            }
        }
        return opClasses;
    }

    private List<String> getOperators(Connection con) throws SQLException {
        List<String> opClasses = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT oprname FROM pg_operator;")) {
                while (rs.next()) {
                    opClasses.add(rs.getString(1));
                }
            }
        }
        return opClasses;
    }

    public List<String> getOperators() {
        return operators;
    }

    public String getRandomOperator() {
        return Randomly.fromList(operators);
    }

    public List<String> getCollates() {
        return collates;
    }

    public String getRandomCollate() {
        return Randomly.fromList(collates);
    }

    public List<String> getOpClasses() {
        return opClasses;
    }

    public String getRandomOpclass() {
        return Randomly.fromList(opClasses);
    }

    protected void updateSchema() throws SQLException {
        setSchema(PostgresSchema.fromConnection(getConnection(), getDatabaseName()));

    public void setVolatilities(HashSet<String> volatileFunctions, HashSet<String> stableFunctions, HashSet<String> immutableFunctions) {
        this.volatileFunctions = volatileFunctions;
        this.stableFunctions = stableFunctions;
        this.immutableFunctions = immutableFunctions;
    }

    public HashSet<String> getVolatileFunctions() {
        return this.volatileFunctions;
    }

    public HashSet<String> getStableFunctions() {
        return this.stableFunctions;
    }

    public HashSet<String> getImmutableFunctions() {
        return this.immutableFunctions;
    }

    public void setAllowVolatileFunction(boolean permission) {
        this.allowVolatileFunction = permission;
    }

    public boolean getAllowVolatileFunction() {
        return this.allowVolatileFunction;
    }

    public void setAllowStableFunction(boolean permission) {
        this.allowStableFunction = permission;
    }

    public boolean getAllowStableFunction() {
        return this.allowStableFunction;
    }

    public void setAllowImmutableFunction(boolean permission) {
        this.allowImmutableFunction = permission;
    }

    public boolean getAllowImmutableFunction() {
        return this.allowImmutableFunction;
    }

}
