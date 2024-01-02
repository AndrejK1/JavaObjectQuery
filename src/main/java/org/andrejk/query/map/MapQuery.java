package org.andrejk.query.map;

import org.andrejk.query.AbstractObjectQuery;

import java.util.*;
import java.util.stream.Collectors;

public class MapQuery<K> extends AbstractObjectQuery<Map<K, Object>, K> {

    @Override
    protected int compareRecords(Map<K, Object> r, Map<K, Object> r2, K key, SortType sortType) {
        Object v = r.get(key);
        Object v2 = r2.get(key);

        if (v == null) {
            return 1;
        }

        if (v2 == null) {
            return -1;
        }

        if (!v.getClass().equals(v2.getClass())) {
            throw new IllegalArgumentException("Can't compare values of different classes: " + v.getClass() + " and " + v2.getClass());
        }

        if (!Comparable.class.isAssignableFrom(v.getClass())) {
            throw new IllegalArgumentException("Values of class " + v.getClass() + " are not comparable!");

        }

        return sortType == SortType.ASC ? ((Comparable) v).compareTo(v2) : ((Comparable) v2).compareTo(v);
    }

    @Override
    protected List<Map<K, Object>> selectFields(List<Map<K, Object>> source, Collection<K> selectedFields) {
        return source.stream()
                .map(map -> {
                    HashMap<K, Object> result = new HashMap<>();
                    selectedFields.forEach(f -> result.put(f, map.get(f)));
                    return result;
                })
                .collect(Collectors.toList());
    }

    @Override
    protected Map<K, Object> constructGroupedRecord(List<K> groupByFields,
                                                    List<Object> groupByFieldValues,
                                                    List<Map<K, Object>> groupedRecords,
                                                    List<GroupByAggregation<Map<K, Object>, K>> groupByAggregations) {
        HashMap<K, Object> groupedRecord = new HashMap<>();

        for (int i = 0; i < groupByFields.size(); i++) {
            groupedRecord.put(groupByFields.get(i), groupByFieldValues.get(i));
        }

        groupByAggregations.forEach(aggregation ->
                groupedRecord.put(aggregation.getAggregationResultField(), aggregation.getAggregationOperation().apply(groupedRecords))
        );

        return groupedRecord;
    }

    @Override
    protected Map<K, Object> join(Map<K, Object> sourceRecord, Map<K, Object> joinedSourceRecord) {
        HashMap<K, Object> joinedMap = Optional.ofNullable(sourceRecord).map(HashMap::new).orElseGet(HashMap::new);

        if (joinedSourceRecord != null) {
            joinedSourceRecord.forEach(joinedMap::putIfAbsent);
        }

        return joinedMap;
    }

    @Override
    protected Object extractValue(Map<K, Object> source, K field) {
        return source.get(field);
    }
}
