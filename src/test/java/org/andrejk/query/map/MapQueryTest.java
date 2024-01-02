package org.andrejk.query.map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.andrejk.query.ObjectQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class MapQueryTest {
    private static List<Map<String, Object>> CUSTOMERS_DATA;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @BeforeAll
    public static void setup() throws IOException {
        CUSTOMERS_DATA = OBJECT_MAPPER.readValue(MapQueryTest.class.getClassLoader().getResourceAsStream("customers.json"), new TypeReference<>() {
        });
    }

    @Test
    void testSelect() {
        List<String> select = List.of("ids", "name", "ageAvg");

        ObjectQuery.WhereGroup<String> where = new ObjectQuery.WhereGroup<>(
                List.of(new ObjectQuery.WhereGroup<>(
                        List.of(),
                        List.of(
                                new ObjectQuery.WhereGroup.WhereCondition<>("name", ObjectQuery.WhereGroup.WhereCondition.ConditionType.CONTAINS, "And"),
                                new ObjectQuery.WhereGroup.WhereCondition<>("name", ObjectQuery.WhereGroup.WhereCondition.ConditionType.CONTAINS, "ov"),
                                new ObjectQuery.WhereGroup.WhereCondition<>("name", ObjectQuery.WhereGroup.WhereCondition.ConditionType.CONTAINS, "Vi")
                        ),
                        ObjectQuery.WhereGroup.GroupConditionType.OR
                )),
                List.of(new ObjectQuery.WhereGroup.WhereCondition<>("age", ObjectQuery.WhereGroup.WhereCondition.ConditionType.LOWER, 25)),
                ObjectQuery.WhereGroup.GroupConditionType.AND
        );

        List<ObjectQuery.Sort<String>> sortMap = List.of(new ObjectQuery.Sort<>("age", ObjectQuery.SortType.DESC),
                new ObjectQuery.Sort<>("name", ObjectQuery.SortType.DESC));

        List<String> groupByFields = List.of("name", "age");
        List<ObjectQuery.GroupByAggregation<Map<String, Object>, String>> groupBy = List.of(
                new ObjectQuery.GroupByAggregation<>("ids", records -> records.stream().map(map -> map.get("id")).map(String::valueOf).collect(Collectors.joining(",", "[", "]"))),
                new ObjectQuery.GroupByAggregation<>("ageAvg", records -> records.stream().map(map -> map.getOrDefault("age", 0).toString()).mapToInt(Integer::valueOf).average().orElse(0D))
        );

        int limitFrom = 0;
        int limitSize = 10;

        List<Map<String, Object>> queryResult = new MapQuery<String>()
                .from(CUSTOMERS_DATA)
                .select(select)
                .where(where)
                .sort(sortMap)
                .groupBy(groupByFields, groupBy)
                .limit(limitFrom, limitSize)
                .execute();

        Assertions.assertNotNull(queryResult);
        // TODO ASSERTIONS
    }

}