/**
 * ObjectQuery.java
 * SQL-like on Java objects operations facade
 *
 * @author Andrii Kononenko
 * @since 02.01.2024
 */

package org.andrejk.query;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public interface ObjectQuery<T, F> {

    /**
     * Specifies the fields to select in the query.
     *
     * @param fields the fields to include in the result
     * @return the current query instance
     */
    ObjectQuery<T, F> select(Collection<F> fields);

    /**
     * Defines the source collection to query from, along with an optional alias.
     *
     * @param source      the data source
     * @param sourceAlias an alias for the source
     * @return the current query instance
     */
    ObjectQuery<T, F> from(Collection<T> source, Object sourceAlias);

    /**
     * Defines the source collection to query from.
     *
     * @param source the data source
     * @return the current query instance
     */
    default ObjectQuery<T, F> from(Collection<T> source) {
        return from(source, null);
    }

    /**
     * Joins another source collection with the current query using the specified join type.
     *
     * @param joinedSource      the collection to join
     * @param joinedSourceAlias an alias for the joined source
     * @param sourceField       the field from the source to join on
     * @param joinedSourceField the field from the joined source to join on
     * @param joinType          the type of join to perform
     * @return the current query instance
     */
    ObjectQuery<T, F> join(Collection<T> joinedSource, Object joinedSourceAlias, F sourceField, F joinedSourceField, JoinType joinType);

    /**
     * Joins another source collection using the default {@link JoinType#INNER}.
     *
     * @param joinedSource      the collection to join
     * @param joinedSourceAlias an alias for the joined source
     * @param sourceField       the field from the source to join on
     * @param joinedSourceField the field from the joined source to join on
     * @return the current query instance
     */
    default ObjectQuery<T, F> join(Collection<T> joinedSource, Object joinedSourceAlias, F sourceField, F joinedSourceField) {
        return join(joinedSource, joinedSourceAlias, sourceField, joinedSourceField, JoinType.INNER);
    }

    /**
     * Adds a where clause to the query.
     *
     * @param whereClause the group of conditions to apply
     * @return the current query instance
     */
    ObjectQuery<T, F> where(WhereGroup<F> whereClause);

    /**
     * Adds a single where condition wrapped in an AND group.
     *
     * @param whereClause the condition to apply
     * @return the current query instance
     */
    default ObjectQuery<T, F> where(WhereGroup.WhereCondition<F> whereClause) {
        return where(new WhereGroup<>(Collections.emptyList(), Collections.singletonList(whereClause), WhereGroup.GroupConditionType.AND));
    }

    /**
     * Limits the result set to a range.
     *
     * @param fromInclusive the start index (inclusive)
     * @param limitSize     the maximum number of results
     * @return the current query instance
     */
    ObjectQuery<T, F> limit(int fromInclusive, int limitSize);

    /**
     * Limits the result set to a number of elements starting from index 0.
     *
     * @param limitSize the maximum number of results
     * @return the current query instance
     */
    default ObjectQuery<T, F> limit(int limitSize) {
        return limit(0, limitSize);
    }

    /**
     * Groups the result set by the specified fields and applies aggregation functions.
     *
     * @param fields       the fields to group by
     * @param aggregations the aggregation operations to apply
     * @return the current query instance
     */
    ObjectQuery<T, F> groupBy(List<F> fields, List<GroupByAggregation<T, F>> aggregations);

    /**
     * Groups the result set by a single field and applies aggregation functions.
     *
     * @param field        the field to group by
     * @param aggregations the aggregation operations to apply
     * @return the current query instance
     */
    default ObjectQuery<T, F> groupBy(F field, List<GroupByAggregation<T, F>> aggregations) {
        return groupBy(List.of(field), aggregations);
    }

    /**
     * Applies sorting to the result set based on the specified fields and sort directions.
     *
     * @param sorts the sort parameters
     * @return the current query instance
     */
    ObjectQuery<T, F> sort(List<Sort<F>> sorts);

    /**
     * Makes the query result distinct by removing duplicate records.
     *
     * @return the current query instance
     */
    ObjectQuery<T, F> distinct();

    /**
     * Executes the query and returns the result as a list.
     *
     * @return the list of resulting objects
     */
    List<T> execute();

    /**
     * Enumeration of supported join types.
     */
    enum JoinType {
        INNER, CROSS, LEFT, RIGHT, FULL, LEFT_EXCLUSIVE, RIGHT_EXCLUSIVE, FULL_EXCLUSIVE
    }

    /**
     * Enumeration of supported sort directions.
     */
    enum SortType {
        ASC, DESC
    }

    /**
     * Represents a sorting instruction on a given field.
     *
     * @param <F> the type of field
     */
    @Getter
    @Builder
    @AllArgsConstructor
    class Sort<F> {
        private F sourceField;
        private SortType sortType;
    }

    /**
     * Represents an aggregation operation for a grouped field.
     *
     * @param <T> the type of the input objects
     * @param <F> the type of the aggregation result field
     */
    @Getter
    @Builder
    @AllArgsConstructor
    class GroupByAggregation<T, F> {
        private F aggregationResultField;
        private Function<Collection<T>, Object> aggregationOperation;
    }

    /**
     * Represents a logical group of where conditions or subgroups.
     *
     * @param <F> the type of field used in conditions
     */
    @Getter
    @Builder
    @AllArgsConstructor
    class WhereGroup<F> {
        private List<WhereGroup<F>> groups;
        private List<WhereCondition<F>> conditions;
        private GroupConditionType groupCondition;

        /**
         * Defines how grouped conditions are combined.
         */
        public enum GroupConditionType {
            AND, OR
        }

        /**
         * A single condition applied to a field.
         *
         * @param <F> the type of the field
         */
        @Getter
        @Builder
        @AllArgsConstructor
        public static class WhereCondition<F> {
            private F field;
            private ConditionType condition;
            private Object value;

            public enum ConditionType {
                EQUALS, NOT_EQUALS, CONTAINS, LOWER, LOWER_EQUALS, BIGGER, BIGGER_EQUALS, IN, IS_NULL, IS_NOT_NULL
            }
        }
    }
}
