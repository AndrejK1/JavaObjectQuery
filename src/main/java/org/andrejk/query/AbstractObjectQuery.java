/**
 * AbstractObjectQuery.java
 * SQL-like on Java objects operations skeleton
 *
 * @author Andrii Kononenko
 * @since 02.01.2024
 */

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
    protected Source<T> source;
    protected WhereGroup<F> whereClause;
    protected Integer limitFrom;
    protected Integer limitSize;
    protected List<Sort<F>> sorts;
    protected List<JoinedSource<T, F>> joinedSources = new ArrayList<>();
    protected List<F> groupByFields;
    protected boolean distinct = false;
    protected List<GroupByAggregation<T, F>> groupByAggregations;

    @Override
    public ObjectQuery<T, F> select(Collection<F> fields) {
        this.selectedFields = fields;
        return this;
    }

    @Override
    public ObjectQuery<T, F> from(Collection<T> source, Object sourceAlias) {
        this.source = new Source<>(List.copyOf(source), sourceAlias);
        return this;
    }

    @Override
    public ObjectQuery<T, F> join(Collection<T> joinedSource, Object joinedSourceAlias, F sourceField, F joinedSourceField, JoinType joinType) {
        joinedSources.add(new JoinedSource<>(List.copyOf(joinedSource), joinedSourceAlias, sourceField, joinedSourceField, joinType));
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
    public ObjectQuery<T, F> distinct() {
        distinct = true;
        return this;
    }

    @Override
    public List<T> execute() {
        if (source == null) {
            throw new IllegalArgumentException("Source cannot be null");
        }

        List<T> intermediateResult = source.getJoinedSourceAlias() == null ?
                new ArrayList<>(source.getSource()) :
                aliasSource(source.getSource(), source.getJoinedSourceAlias());

        if (!joinedSources.isEmpty()) {
            for (JoinedSource<T, F> joinedSource : joinedSources) {
                intermediateResult = joinSource(
                        intermediateResult,
                        joinedSource.getSourceField(),
                        aliasSource(joinedSource.getJoinedSource(), joinedSource.getJoinedSourceAlias()),
                        joinedSource.getJoinedSourceField(),
                        joinedSource.joinType
                );
            }
        }

        if (whereClause != null) {
            intermediateResult = intermediateResult.stream()
                    .filter(record -> testRecord(record, whereClause))
                    .collect(Collectors.toList());
        }

        if (groupByFields != null && !groupByFields.isEmpty()) {
            intermediateResult = intermediateResult.stream()
                    .collect(Collectors.groupingBy(record -> constructGroupingKey(record, groupByFields)))
                    .entrySet()
                    .stream()
                    .map(groupedRecords -> constructGroupedRecord(groupByFields, groupedRecords.getKey(), groupedRecords.getValue(), groupByAggregations))
                    .collect(Collectors.toList());
        }

        if (sorts != null) {
            intermediateResult.sort(constructComparator(sorts));
        }

        if (selectedFields != null && !selectedFields.isEmpty()) {
            intermediateResult = selectFields(intermediateResult, selectedFields);
        }

        if (distinct) {
            intermediateResult = intermediateResult.stream()
                    .distinct()
                    .collect(Collectors.toList());
        }

        if (limitSize != null) {
            return intermediateResult.stream()
                    .skip(limitFrom)
                    .limit(limitSize)
                    .collect(Collectors.toList());
        }

        return intermediateResult;
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

    protected boolean testRecord(T record, WhereGroup<F> whereClause) {
        Stream<Boolean> conditions = Stream.concat(
                whereClause.getGroups().stream().map(group -> testRecord(record, group)),
                whereClause.getConditions().stream().map(condition -> testCondition(record, condition))
        );

        return whereClause.getGroupCondition() == WhereGroup.GroupConditionType.AND ?
                conditions.allMatch(b -> b) :
                conditions.anyMatch(b -> b);
    }

    protected Boolean testCondition(T record, WhereGroup.WhereCondition<F> condition) {
        Object sourceValue = extractValue(record, condition.getField());

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
                return sourceValue != null && compareNumbers(condition, sourceValue) < 0;
            case LOWER_EQUALS:
                return sourceValue != null && compareNumbers(condition, sourceValue) <= 0;
            case BIGGER:
                return sourceValue != null && compareNumbers(condition, sourceValue) > 0;
            case BIGGER_EQUALS:
                return sourceValue != null && compareNumbers(condition, sourceValue) >= 0;
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

    protected List<T> joinSource(List<T> baseSource, F baseSourceField, List<T> joinedSource, F joinedSourceField, JoinType joinType) {
        switch (joinType) {
            case INNER:
                return innerJoin(baseSource, baseSourceField, joinedSource, joinedSourceField);
            case CROSS:
                return crossJoin(baseSource, joinedSource);
            case LEFT:
                return leftJoin(baseSource, baseSourceField, joinedSource, joinedSourceField, true);
            case RIGHT:
                return leftJoin(joinedSource, joinedSourceField, baseSource, baseSourceField, true);
            case FULL:
                return fullJoin(baseSource, baseSourceField, joinedSource, joinedSourceField, true);
            case LEFT_EXCLUSIVE:
                return leftJoin(baseSource, baseSourceField, joinedSource, joinedSourceField, false);
            case RIGHT_EXCLUSIVE:
                return leftJoin(joinedSource, joinedSourceField, baseSource, baseSourceField, false);
            case FULL_EXCLUSIVE:
                return fullJoin(baseSource, baseSourceField, joinedSource, joinedSourceField, false);
            default:
                throw new IllegalArgumentException(joinType + " join type is not supported");
        }
    }

    protected List<T> innerJoin(List<T> baseSource, F baseSourceField, List<T> joinedSource, F joinedSourceField) {
        List<T> joinedResult = new ArrayList<>();

        for (T sourceRecord : baseSource) {
            Object sourceValue = extractValue(sourceRecord, baseSourceField);

            for (T joinedSourceRecord : joinedSource) {
                Object joinedSourceValue = extractValue(joinedSourceRecord, joinedSourceField);

                if (sourceValue != null && sourceValue.equals(joinedSourceValue)) {
                    joinedResult.add(join(sourceRecord, joinedSourceRecord));
                }
            }
        }

        return joinedResult;
    }

    protected List<T> crossJoin(List<T> baseSource, List<T> joinedSource) {
        List<T> joinedResult = new ArrayList<>();

        for (T sourceRecord : baseSource) {
            for (T joinedSourceRecord : joinedSource) {
                joinedResult.add(join(sourceRecord, joinedSourceRecord));
            }
        }

        return joinedResult;
    }

    protected List<T> leftJoin(List<T> baseSource, F baseSourceField, List<T> joinedSource, F joinedSourceField, boolean includeMatching) {
        List<T> joinedResult = new ArrayList<>();

        for (T sourceRecord : baseSource) {
            boolean recordJoined = false;

            Object sourceValue = extractValue(sourceRecord, baseSourceField);

            for (T joinedSourceRecord : joinedSource) {
                Object joinedSourceValue = extractValue(joinedSourceRecord, joinedSourceField);

                if (sourceValue != null && sourceValue.equals(joinedSourceValue)) {
                    if (includeMatching) {
                        joinedResult.add(join(sourceRecord, joinedSourceRecord));
                    }
                    recordJoined = true;
                }
            }

            if (!recordJoined) {
                joinedResult.add(join(sourceRecord, null));
            }
        }

        return joinedResult;
    }

    private List<T> fullJoin(List<T> baseSource, F baseSourceField, List<T> joinedSource, F joinedSourceField, boolean includeMatching) {
        List<T> joinedResult = new ArrayList<>();

        boolean[] joinedSourceRecordUsed = new boolean[joinedSource.size()];

        for (T sourceRecord : baseSource) {
            boolean recordJoined = false;

            Object sourceValue = extractValue(sourceRecord, baseSourceField);

            for (int i = 0; i < joinedSource.size(); i++) {
                T joinedSourceRecord = joinedSource.get(i);
                Object joinedSourceValue = extractValue(joinedSourceRecord, joinedSourceField);

                if (sourceValue != null && sourceValue.equals(joinedSourceValue)) {
                    if (includeMatching) {
                        joinedResult.add(join(sourceRecord, joinedSourceRecord));
                    }

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
                joinedResult.add(join(joinedSource.get(i), null));
            }
        }

        return joinedResult;
    }

    protected abstract List<T> aliasSource(List<T> source, Object joinedSourceAlias);

    abstract protected int compareRecords(T r, T r2, F key, SortType sortType);

    abstract protected List<T> selectFields(List<T> source, Collection<F> selectedFields);

    abstract protected T constructGroupedRecord(List<F> groupByFields,
                                                List<Object> groupByFieldValues,
                                                List<T> groupedRecords,
                                                List<GroupByAggregation<T, F>> groupByAggregations);

    abstract protected T join(T sourceRecord, T joinedSourceRecord);

    abstract protected Object extractValue(T source, F field);

    @Getter
    @Builder
    @AllArgsConstructor
    protected static class Source<T> {
        private List<T> source;
        private Object joinedSourceAlias;
    }

    @Getter
    @Builder
    @AllArgsConstructor
    protected static class JoinedSource<T, F> {
        private List<T> joinedSource;
        private Object joinedSourceAlias;
        private F sourceField;
        private F joinedSourceField;
        private JoinType joinType;
    }
}
