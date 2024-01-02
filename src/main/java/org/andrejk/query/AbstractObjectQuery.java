package org.andrejk.query;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractObjectQuery<T, F> implements ObjectQuery<T, F> {
    protected Collection<F> selectedFields;
    protected List<T> source;
    protected WhereGroup<F> whereClause;
    protected Integer limitFrom;
    protected Integer limitSize;
    protected List<Sort<F>> sorts;
    protected List<JoinedSource<T, F>> joinedSources = new ArrayList<>();
    protected List<F> groupByFields;
    protected List<GroupByAggregation<T, F>> groupByAggregations;

    @Override
    public ObjectQuery<T, F> select(Collection<F> fields) {
        this.selectedFields = fields;
        return this;
    }

    @Override
    public ObjectQuery<T, F> from(Collection<T> source) {
        this.source = new ArrayList<>(source);
        return this;
    }

    @Override
    public ObjectQuery<T, F> join(Collection<T> joinedSource, F sourceField, F joinedSourceField, JoinType joinType) {
        joinedSources.add(new JoinedSource<>(new ArrayList<>(joinedSource), sourceField, joinedSourceField, joinType));
        return this;
    }

    @Override
    public ObjectQuery<T, F> where(WhereGroup<F> whereClause) {
        this.whereClause = whereClause;
        return this;
    }

    @Override
    public ObjectQuery<T, F> limit(int fromInclusive, int limitSize) {
        this.limitFrom = fromInclusive;
        this.limitSize = limitSize;
        return this;
    }

    @Override
    public ObjectQuery<T, F> groupBy(List<F> fields, List<GroupByAggregation<T, F>> aggregations) {
        this.groupByFields = fields;
        this.groupByAggregations = aggregations;
        return this;
    }

    @Override
    public ObjectQuery<T, F> sort(List<Sort<F>> sorts) {
        this.sorts = sorts;
        return this;
    }

    @Override
    public List<T> execute() {
        if (source == null) {
            throw new IllegalArgumentException("Source cannot be null");
        }

        if (!joinedSources.isEmpty()) {
            for (JoinedSource<T, F> joinedSource : joinedSources) {
                source = joinSource(source, joinedSource);
            }
        }

        if (whereClause != null) {
            source = source.stream()
                    .filter(record -> testRecord(record, whereClause))
                    .collect(Collectors.toList());
        }

        if (groupByFields != null && !groupByFields.isEmpty()) {
            source = source.stream()
                    .collect(Collectors.groupingBy(record -> constructGroupingKey(record, groupByFields)))
                    .entrySet()
                    .stream()
                    .map(groupedRecords -> constructGroupedRecord(groupByFields, groupedRecords.getKey(), groupedRecords.getValue(), groupByAggregations))
                    .collect(Collectors.toList());
        }

        if (sorts != null) {
            source.sort(constructComparator(sorts));
        }

        if (selectedFields != null && !selectedFields.isEmpty()) {
            source = selectFields(source, selectedFields);
        }

        if (limitSize != null) {
            return source.stream()
                    .skip(limitFrom)
                    .limit(limitSize)
                    .collect(Collectors.toList());
        }

        return source;
    }

    protected List<Object> constructGroupingKey(T record, List<F> groupByFields) {
        return groupByFields.stream()
                .map(f -> extractValue(record, f))
                .collect(Collectors.toList());
    }

    protected Comparator<T> constructComparator(List<Sort<F>> sorts) {
        return (r, r2) -> {
            for (Sort<F> sort : sorts) {
                int result = compareRecords(r, r2, sort.getSourceField(), sort.getSortType());

                if (result != 0) {
                    return result;
                }
            }
            return 0;
        };
    }

    protected boolean testRecord(T source, WhereGroup<F> whereClause) {
        Stream<Boolean> conditions = Stream.concat(
                whereClause.getGroups().stream().map(group -> testRecord(source, group)),
                whereClause.getConditions().stream().map(condition -> testCondition(source, condition))
        );

        return whereClause.getGroupCondition() == WhereGroup.GroupConditionType.AND ?
                conditions.allMatch(b -> b) :
                conditions.anyMatch(b -> b);
    }

    protected Boolean testCondition(T source, WhereGroup.WhereCondition<F> condition) {
        Object sourceValue = extractValue(source, condition.getField());

        switch (condition.getCondition()) {
            case EQUALS:
                return sourceValue != null && sourceValue.equals(condition.getValue());
            case NOT_EQUALS:
                return sourceValue != null && !sourceValue.equals(condition.getValue());
            case CONTAINS:
                if (!(sourceValue instanceof String)) {
                    throw new IllegalArgumentException("Can't apply CONTAINS condition to " + sourceValue.getClass() + " type");
                }

                if (!(condition.getValue() instanceof String)) {
                    throw new IllegalArgumentException("CONTAINS condition value must be String type");
                }

                return sourceValue.toString().contains(condition.getValue().toString());
            case LOWER:
                return compareNumbers(condition, sourceValue) < 0;
            case LOWER_EQUALS:
                return compareNumbers(condition, sourceValue) <= 0;
            case BIGGER:
                return compareNumbers(condition, sourceValue) > 0;
            case BIGGER_EQUALS:
                return compareNumbers(condition, sourceValue) >= 0;
            case IN:
                if (!(condition.getValue() instanceof Collection)) {
                    throw new IllegalArgumentException("CONTAINS condition value must be Collection type");
                }

                return ((Collection<?>) condition.getValue()).contains(sourceValue);
            case IS_NULL:
                return sourceValue == null;
            case IS_NOT_NULL:
                return sourceValue != null;
        }

        throw new IllegalArgumentException("Can't process " + condition.getCondition() + " type");
    }

    private int compareNumbers(WhereGroup.WhereCondition<F> condition, Object sourceValue) {
        if (!(sourceValue instanceof Number)) {
            throw new IllegalArgumentException("Can't apply " + condition.getCondition() + " condition to " + sourceValue.getClass() + " type");
        }

        if (!(condition.getValue() instanceof Number)) {
            throw new IllegalArgumentException(condition.getCondition() + " condition value must be Number type");
        }

        return Double.compare(((Number) sourceValue).doubleValue(), ((Number) condition.getValue()).doubleValue());
    }

    protected List<T> joinSource(List<T> source, JoinedSource<T, F> joinedSource) {
        switch (joinedSource.getJoinType()) {
            case INNER:
                return innerJoin(source, joinedSource);
            case LEFT:
                return leftJoin(source, joinedSource);
            case RIGHT:
                return leftJoin(joinedSource.getSource(), new JoinedSource<>(source, joinedSource.getJoinedSourceField(), joinedSource.getSourceField(), JoinType.LEFT));
            case CROSS:
                return crossJoin(source, joinedSource);
            case FULL:
                return fullJoin(source, joinedSource);
            case LEFT_EXCLUSIVE:
                return leftExclusiveJoin(source, joinedSource);
            case RIGHT_EXCLUSIVE:
                return leftExclusiveJoin(joinedSource.getSource(), new JoinedSource<>(source, joinedSource.getJoinedSourceField(), joinedSource.getSourceField(), JoinType.LEFT_EXCLUSIVE));
            case FULL_EXCLUSIVE:
                return fullExclusiveJoin(source, joinedSource);
            default:
                throw new IllegalArgumentException(joinedSource.getJoinType() + " join type is not supported");
        }
    }

    protected List<T> innerJoin(List<T> source, JoinedSource<T, F> joinedSource) {
        List<T> joinedResult = new ArrayList<>();

        for (T sourceRecord : source) {
            Object sourceValue = extractValue(sourceRecord, joinedSource.getSourceField());

            for (T joinedSourceRecord : joinedSource.getSource()) {
                Object joinedSourceValue = extractValue(joinedSourceRecord, joinedSource.getJoinedSourceField());

                if (sourceValue != null && sourceValue.equals(joinedSourceValue)) {
                    joinedResult.add(join(sourceRecord, joinedSourceRecord));
                }
            }
        }

        return joinedResult;
    }

    protected List<T> crossJoin(List<T> source, JoinedSource<T, F> joinedSource) {
        List<T> joinedResult = new ArrayList<>();

        for (T sourceRecord : source) {
            for (T joinedSourceRecord : joinedSource.getSource()) {
                joinedResult.add(join(sourceRecord, joinedSourceRecord));
            }
        }

        return joinedResult;
    }

    protected List<T> leftJoin(List<T> source, JoinedSource<T, F> joinedSource) {
        List<T> joinedResult = new ArrayList<>();

        for (T sourceRecord : source) {
            boolean recordJoined = false;

            Object sourceValue = extractValue(sourceRecord, joinedSource.getSourceField());

            for (T joinedSourceRecord : joinedSource.getSource()) {
                Object joinedSourceValue = extractValue(joinedSourceRecord, joinedSource.getJoinedSourceField());

                if (sourceValue != null && sourceValue.equals(joinedSourceValue)) {
                    joinedResult.add(join(sourceRecord, joinedSourceRecord));
                    recordJoined = true;
                }
            }

            if (!recordJoined) {
                joinedResult.add(join(sourceRecord, null));
            }
        }

        return joinedResult;
    }

    private List<T> fullJoin(List<T> source, JoinedSource<T, F> joinedSource) {
        List<T> joinedResult = new ArrayList<>();
        List<T> joinedSourceRecords = joinedSource.getSource();

        boolean[] joinedSourceRecordUsed = new boolean[joinedSourceRecords.size()];

        for (T sourceRecord : source) {
            boolean recordJoined = false;

            Object sourceValue = extractValue(sourceRecord, joinedSource.getSourceField());

            for (int i = 0; i < joinedSourceRecords.size(); i++) {
                T joinedSourceRecord = joinedSourceRecords.get(i);
                Object joinedSourceValue = extractValue(joinedSourceRecord, joinedSource.getJoinedSourceField());

                if (sourceValue != null && sourceValue.equals(joinedSourceValue)) {
                    joinedResult.add(join(sourceRecord, joinedSourceRecord));
                    recordJoined = true;
                    joinedSourceRecordUsed[i] = true;
                }
            }

            if (!recordJoined) {
                joinedResult.add(join(sourceRecord, null));
            }
        }

        for (int i = 0; i < joinedSourceRecordUsed.length; i++) {
            if (!joinedSourceRecordUsed[i]) {
                joinedResult.add(join(joinedSourceRecords.get(i), null));
            }
        }

        return joinedResult;
    }

    private List<T> leftExclusiveJoin(List<T> source, JoinedSource<T, F> joinedSource) {
        List<T> joinedResult = new ArrayList<>();

        for (T sourceRecord : source) {
            boolean recordJoined = false;

            Object sourceValue = extractValue(sourceRecord, joinedSource.getSourceField());

            for (T joinedSourceRecord : joinedSource.getSource()) {
                Object joinedSourceValue = extractValue(joinedSourceRecord, joinedSource.getJoinedSourceField());

                if (sourceValue != null && sourceValue.equals(joinedSourceValue)) {
                    recordJoined = true;
                }
            }

            if (!recordJoined) {
                joinedResult.add(join(sourceRecord, null));
            }
        }

        return joinedResult;
    }

    private List<T> fullExclusiveJoin(List<T> source, JoinedSource<T, F> joinedSource) {
        List<T> joinedResult = new ArrayList<>();
        List<T> joinedSourceRecords = joinedSource.getSource();

        boolean[] joinedSourceRecordUsed = new boolean[joinedSourceRecords.size()];

        for (T sourceRecord : source) {
            boolean recordJoined = false;

            Object sourceValue = extractValue(sourceRecord, joinedSource.getSourceField());

            for (int i = 0; i < joinedSourceRecords.size(); i++) {
                T joinedSourceRecord = joinedSourceRecords.get(i);
                Object joinedSourceValue = extractValue(joinedSourceRecord, joinedSource.getJoinedSourceField());

                if (sourceValue != null && sourceValue.equals(joinedSourceValue)) {
                    recordJoined = true;
                    joinedSourceRecordUsed[i] = true;
                }
            }

            if (!recordJoined) {
                joinedResult.add(join(sourceRecord, null));
            }
        }

        for (int i = 0; i < joinedSourceRecordUsed.length; i++) {
            if (!joinedSourceRecordUsed[i]) {
                joinedResult.add(join(joinedSourceRecords.get(i), null));
            }
        }

        return joinedResult;
    }

    abstract protected int compareRecords(T r, T r2, F key, SortType sortType);

    abstract protected List<T> selectFields(List<T> source, Collection<F> selectedFields);

    abstract protected T constructGroupedRecord(List<F> groupByFields, List<Object> groupByFieldValues, List<T> groupedRecords, List<GroupByAggregation<T, F>> groupByAggregations);

    abstract protected T join(T sourceRecord, T joinedSourceRecord);

    abstract protected Object extractValue(T source, F field);

    @Getter
    @Builder
    @AllArgsConstructor
    protected static class JoinedSource<T, F> {
        private List<T> source;
        private F sourceField;
        private F joinedSourceField;
        private JoinType joinType;
    }
}
