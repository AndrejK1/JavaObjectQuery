package org.andrejk.query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class MapQueryTest {
    private static List<Map<String, Object>> CUSTOMERS_MISSING_DATA;
    private static List<Map<String, Object>> CUSTOMERS_DATA;
    private static List<Map<String, Object>> CITIES_DATA;

    @BeforeAll
    public static void setup() throws IOException {
        CUSTOMERS_DATA = TestUtils.readJsonFileToListOfMaps("customers.json");
        CUSTOMERS_MISSING_DATA = TestUtils.readJsonFileToListOfMaps("customers_missing_data.json");
        CITIES_DATA = TestUtils.readJsonFileToListOfMaps("cities.json");
    }

    @Test
    void showCaseTest() {
        ObjectQuery.WhereGroup<String> where = new ObjectQuery.WhereGroup<>(
                List.of(new ObjectQuery.WhereGroup<>(
                        List.of(),
                        List.of(
                                new ObjectQuery.WhereGroup.WhereCondition<>("customers.name", ObjectQuery.WhereGroup.WhereCondition.ConditionType.CONTAINS, "And"),
                                new ObjectQuery.WhereGroup.WhereCondition<>("customers.name", ObjectQuery.WhereGroup.WhereCondition.ConditionType.CONTAINS, "ov"),
                                new ObjectQuery.WhereGroup.WhereCondition<>("customers.name", ObjectQuery.WhereGroup.WhereCondition.ConditionType.CONTAINS, "Vi")
                        ),
                        ObjectQuery.WhereGroup.GroupConditionType.OR
                )),
                List.of(new ObjectQuery.WhereGroup.WhereCondition<>("customers.age", ObjectQuery.WhereGroup.WhereCondition.ConditionType.LOWER_EQUALS, 24)),
                ObjectQuery.WhereGroup.GroupConditionType.AND
        );

        List<ObjectQuery.GroupByAggregation<Map<String, Object>, String>> groupBy = List.of(
                new ObjectQuery.GroupByAggregation<>("ids", records -> records.stream().map(map -> map.get("customers.id")).map(String::valueOf).collect(Collectors.joining(",", "[", "]"))),
                new ObjectQuery.GroupByAggregation<>("cityNames", records -> records.stream().map(map -> map.get("cities.name")).map(String::valueOf).collect(Collectors.joining(",", "[", "]"))),
                new ObjectQuery.GroupByAggregation<>("ageAvg", records -> records.stream().map(map -> map.getOrDefault("customers.age", 0).toString()).mapToInt(Integer::valueOf).average().orElse(0D))
        );

        int limitFrom = 0;
        int limitSize = 10;

        List<Map<String, Object>> queryResult = new MapQuery<>(MapQuery.STRING_KEY_ALIASING_FUNCTION)
                .distinct()
                .select(List.of("customers.name", "ageAvg", "cityNames"))
                .from(CUSTOMERS_DATA, "customers")
                .join(CITIES_DATA, "cities", "customers.cityId", "cities.id")
                .where(where)
                .groupBy(List.of("customers.name"), groupBy)
                .sort(List.of(new ObjectQuery.Sort<>("ageAvg", ObjectQuery.SortType.DESC)))
                .limit(limitFrom, limitSize)
                .execute();

        Assertions.assertNotNull(queryResult);
        Assertions.assertEquals(2, queryResult.size());

        Assertions.assertNotNull(queryResult.getFirst());
        Assertions.assertEquals("Vova", queryResult.getFirst().get("customers.name"));
        Assertions.assertEquals(24.0, queryResult.getFirst().get("ageAvg"));
        Assertions.assertEquals("[New York]", queryResult.getFirst().get("cityNames"));

        Assertions.assertNotNull(queryResult.get(1));
        Assertions.assertEquals("Andrii", queryResult.get(1).get("customers.name"));
        Assertions.assertEquals(22.5, queryResult.get(1).get("ageAvg"));
        Assertions.assertEquals("[Florida,London]", queryResult.get(1).get("cityNames"));
    }

    @Test
    void leftJoinTest() {
        List<Map<String, Object>> queryResult = new MapQuery<>(MapQuery.STRING_KEY_ALIASING_FUNCTION)
                .distinct()
                .select(List.of("customers.name", "cities.name"))
                .from(CUSTOMERS_MISSING_DATA, "customers")
                .join(CITIES_DATA, "cities", "customers.cityId", "cities.id", ObjectQuery.JoinType.LEFT)
                .sort(List.of(new ObjectQuery.Sort<>("customers.name", ObjectQuery.SortType.ASC)))
                .execute();

        Assertions.assertNotNull(queryResult);
        Assertions.assertEquals(4, queryResult.size());

        Assertions.assertNotNull(queryResult.getFirst());
        Assertions.assertEquals("Andrii", queryResult.getFirst().get("customers.name"));
        Assertions.assertEquals("New York", queryResult.getFirst().get("cities.name"));

        Assertions.assertNotNull(queryResult.get(1));
        Assertions.assertEquals("Dmytro", queryResult.get(1).get("customers.name"));
        Assertions.assertNull(queryResult.get(1).get("cities.name"));

        Assertions.assertNotNull(queryResult.get(2));
        Assertions.assertEquals("Vova", queryResult.get(2).get("customers.name"));
        Assertions.assertNull(queryResult.get(2).get("cities.name"));

        Assertions.assertNotNull(queryResult.get(3));
        Assertions.assertNull(queryResult.get(3).get("customers.name"));
        Assertions.assertEquals("Florida", queryResult.get(3).get("cities.name"));
    }

}