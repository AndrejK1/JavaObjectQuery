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

    ObjectQuery<T, F> select(Collection<F> fields);

    ObjectQuery<T, F> from(Collection<T> source, Object sourceAlias);

    default ObjectQuery<T, F> from(Collection<T> source) {
        return from(source, null);
    }

    ObjectQuery<T, F> join(Collection<T> joinedSource, Object joinedSourceAlias, F sourceField, F joinedSourceField, JoinType joinType);

    default ObjectQuery<T, F> join(Collection<T> joinedSource, Object joinedSourceAlias, F sourceField, F joinedSourceField) {
        return join(joinedSource, joinedSourceAlias, sourceField, joinedSourceField, JoinType.INNER);
    }

    ObjectQuery<T, F> where(WhereGroup<F> whereClause);

    default ObjectQuery<T, F> where(WhereGroup.WhereCondition<F> whereClause) {
        return where(new WhereGroup<>(Collections.emptyList(), Collections.singletonList(whereClause), WhereGroup.GroupConditionType.AND));
    }

    ObjectQuery<T, F> limit(int fromInclusive, int limitSize);

    default ObjectQuery<T, F> limit(int limitSize) {
        return limit(0, limitSize);
    }

    ObjectQuery<T, F> groupBy(List<F> fields, List<GroupByAggregation<T, F>> aggregations);

    default ObjectQuery<T, F> groupBy(F fields, List<GroupByAggregation<T, F>> aggregations) {
        return groupBy(List.of(fields), aggregations);
    }

    ObjectQuery<T, F> sort(List<Sort<F>> sorts);

    ObjectQuery<T, F> distinct();

    List<T> execute();

    enum JoinType {
        INNER, CROSS, LEFT, RIGHT, FULL, LEFT_EXCLUSIVE, RIGHT_EXCLUSIVE, FULL_EXCLUSIVE
    }

    enum SortType {
        ASC, DESC
    }

    @Getter
    @Builder
    @AllArgsConstructor
    class Sort<F> {
        private F sourceField;
        private SortType sortType;
    }

    @Getter
    @Builder
    @AllArgsConstructor
    class GroupByAggregation<T, F> {
        private F aggregationResultField;
        private Function<Collection<T>, Object> aggregationOperation;
    }

    @Getter
    @Builder
    @AllArgsConstructor
    class WhereGroup<F> {
        private List<WhereGroup<F>> groups;
        private List<WhereCondition<F>> conditions;
        private GroupConditionType groupCondition;

        public enum GroupConditionType {
            AND, OR
        }

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
