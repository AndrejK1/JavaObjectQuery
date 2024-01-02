package org.andrejk.query.map;

import org.andrejk.query.AbstractObjectQuery;

import java.util.*;
import java.util.stream.Collectors;

public class ValueMapQuery<K, V> extends AbstractObjectQuery<Map<K, V>, K> {

    @Override
    protected int compareRecords(Map<K, V> r, Map<K, V> r2, K key, SortType sortType) {
        V v = r.get(key);
        V v2 = r2.get(key);

        if (v == null) {
            return 1;
        }

        if (v2 == null) {
            return -1;
        }

        if (!Comparable.class.isAssignableFrom(v.getClass())) {
            throw new IllegalArgumentException("Values of class " + v.getClass() + " are not comparable!");
        }

        return sortType == SortType.ASC ? ((Comparable) v).compareTo(v2) : ((Comparable) v2).compareTo(v);
    }

    @Override
    protected List<Map<K, V>> selectFields(List<Map<K, V>> source, Collection<K> selectedFields) {
        return source.stream()
                .map(map -> {
                    HashMap<K, V> result = new HashMap<>();
                    selectedFields.forEach(f -> result.put(f, map.get(f)));
                    return result;
                })
                .collect(Collectors.toList());
    }

    @Override
    protected Map<K, V> constructGroupedRecord(List<K> groupByFields, List<Object> groupByFieldValues, List<Map<K, V>> groupedRecords, List<GroupByAggregation<Map<K, V>, K>> groupByAggregations) {
        HashMap<K, V> groupedRecord = new HashMap<>();

        for (int i = 0; i < groupByFields.size(); i++) {
            groupedRecord.put(groupByFields.get(i), (V) groupByFieldValues.get(i));
        }

        groupByAggregations.forEach(aggregation ->
                groupedRecord.put(aggregation.getAggregationResultField(), (V) aggregation.getAggregationOperation().apply(groupedRecords))
        );

        return groupedRecord;
    }

    @Override
    protected Map<K, V> join(Map<K, V> sourceRecord, Map<K, V> joinedSourceRecord) {
        HashMap<K, V> joinedMap = Optional.ofNullable(sourceRecord).map(HashMap::new).orElseGet(HashMap::new);

        if (joinedSourceRecord != null) {
            joinedSourceRecord.forEach(joinedMap::putIfAbsent);
        }

        return joinedMap;
    }

    @Override
    protected Object extractValue(Map<K, V> source, K field) {
        return source.get(field);
    }
}
