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
        List<String> select = List.of("customers.name", "ageAvg", "cityNames");

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

        List<ObjectQuery.Sort<String>> sortMap = List.of(new ObjectQuery.Sort<>("customers.age", ObjectQuery.SortType.DESC),
                new ObjectQuery.Sort<>("customers.name", ObjectQuery.SortType.DESC));

        List<String> groupByFields = List.of("customers.name", "customers.age");
        List<ObjectQuery.GroupByAggregation<Map<String, Object>, String>> groupBy = List.of(
                new ObjectQuery.GroupByAggregation<>("ids", records -> records.stream().map(map -> map.get("customers.id")).map(String::valueOf).collect(Collectors.joining(",", "[", "]"))),
                new ObjectQuery.GroupByAggregation<>("cityNames", records -> records.stream().map(map -> map.get("cities.cityName")).map(String::valueOf).collect(Collectors.joining(",", "[", "]"))),
                new ObjectQuery.GroupByAggregation<>("ageAvg", records -> records.stream().map(map -> map.getOrDefault("customers.age", 0).toString()).mapToInt(Integer::valueOf).average().orElse(0D))
        );

        int limitFrom = 0;
        int limitSize = 10;

        List<Map<String, Object>> queryResult = new MapQuery<>(MapQuery.STRING_KEY_ALIASING_FUNCTION)
                .distinct()
                .select(select)
                .from(CUSTOMERS_DATA, "customers")
                .join(CITIES_DATA, "cities", "customers.city", "cities.cityId")
                .where(where)
                .groupBy(groupByFields, groupBy)
                .sort(sortMap)
                .limit(limitFrom, limitSize)
                .execute();

        Assertions.assertNotNull(queryResult);
        // TODO ASSERTIONS
    }

}