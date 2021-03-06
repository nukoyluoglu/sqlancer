package sqlancer.common.schema;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.QueryAdapter;
import sqlancer.common.query.SQLancerResultSet;

public class AbstractTable<C extends AbstractTableColumn<?, ?>, I extends TableIndex>
        implements Comparable<AbstractTable<?, ?>> {

    private static final int NO_ROW_COUNT_AVAILABLE = -1;
    private final String name;
    private final List<C> columns;
    private final List<I> indexes;
    private final boolean isView;
    private long rowCount = NO_ROW_COUNT_AVAILABLE;

    public AbstractTable(String name, List<C> columns, List<I> indexes, boolean isView) {
        this.name = name;
        this.indexes = indexes;
        this.isView = isView;
        this.columns = Collections.unmodifiableList(columns);
    }

    public String getName() {
        return name;
    }

    @Override
    public int compareTo(AbstractTable<?, ?> o) {
        return o.getName().compareTo(getName());
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getName());
        sb.append("\n");
        for (C c : columns) {
            sb.append("\t" + c + "\n");
        }
        return sb.toString();
    }

    public List<I> getIndexes() {
        return indexes;
    }

    public List<C> getColumns() {
        return columns;
    }

    public String getColumnsAsString() {
        return columns.stream().map(c -> c.getName()).collect(Collectors.joining(", "));
    }

    public String getColumnsAsString(Function<C, String> function) {
        return columns.stream().map(function).collect(Collectors.joining(", "));
    }

    public C getRandomColumn() {
        return Randomly.fromList(columns);
    }

    public boolean hasIndexes() {
        return !indexes.isEmpty();
    }

    public TableIndex getRandomIndex() {
        return Randomly.fromList(indexes);
    }

    public List<C> getRandomNonEmptyColumnSubset() {
        return Randomly.nonEmptySubset(getColumns());
    }

    public List<C> getRandomNonEmptyColumnSubset(int size) {
        return Randomly.nonEmptySubset(getColumns(), size);
    }

    public boolean isView() {
        return isView;
    }

    public String getFreeColumnName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String columnName = String.format("c%d", i++);
            if (columns.stream().noneMatch(t -> t.getName().contentEquals(columnName))) {
                return columnName;
            }
        } while (true);

    }

    public long getNrRows(GlobalState<?, ?> globalState) {
        if (rowCount == NO_ROW_COUNT_AVAILABLE) {
            QueryAdapter q = new QueryAdapter("SELECT COUNT(*) FROM " + name);
            try (SQLancerResultSet query = q.executeAndGet(globalState)) {
                if (query == null) {
                    throw new IgnoreMeException();
                }
                query.next();
                rowCount = query.getLong(1);
                return rowCount;
            } catch (Throwable t) {
                // an exception might be expected, for example, when invalid view is created
                throw new IgnoreMeException();
            }
        } else {
            return rowCount;
        }
    }
}
