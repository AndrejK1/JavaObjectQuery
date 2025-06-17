package org.andrejk.query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void shouldWorkWithAllFeaturesUsed() {
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

        assertNotNull(queryResult);
        assertEquals(1, queryResult.size());

        assertNotNull(queryResult.getFirst());
        assertEquals("Andrii", queryResult.getFirst().get("customers.name"));
        assertEquals(24.0, queryResult.getFirst().get("ageAvg"));
        assertEquals("[New York]", queryResult.getFirst().get("cityNames"));
    }

    @Test
    void shouldJoinCustomersWithCitiesUsingLeftJoin() {
        List<Map<String, Object>> queryResult = new MapQuery<>(MapQuery.STRING_KEY_ALIASING_FUNCTION)
                .distinct()
                .select(List.of("customers.name", "cities.name"))
                .from(CUSTOMERS_MISSING_DATA, "customers")
                .join(CITIES_DATA, "cities", "customers.cityId", "cities.id", ObjectQuery.JoinType.LEFT)
                .sort(List.of(new ObjectQuery.Sort<>("customers.name", ObjectQuery.SortType.ASC)))
                .execute();

        assertNotNull(queryResult);
        assertEquals(4, queryResult.size());

        assertNotNull(queryResult.getFirst());
        assertEquals("Andrii", queryResult.getFirst().get("customers.name"));
        assertEquals("New York", queryResult.getFirst().get("cities.name"));

        assertNotNull(queryResult.get(1));
        assertEquals("Dmytro", queryResult.get(1).get("customers.name"));
        assertNull(queryResult.get(1).get("cities.name"));

        assertNotNull(queryResult.get(2));
        assertEquals("Vova", queryResult.get(2).get("customers.name"));
        assertNull(queryResult.get(2).get("cities.name"));

        assertNotNull(queryResult.get(3));
        assertNull(queryResult.get(3).get("customers.name"));
        assertEquals("Florida", queryResult.get(3).get("cities.name"));
    }

    @Test
    void shouldSelectOnlyNameField() {
        List<Map<String, Object>> result = new MapQuery<String, Object>()
                .select(List.of("name"))
                .from(CUSTOMERS_DATA)
                .execute();

        assertEquals("Andrii", result.getFirst().get("name"));
        assertFalse(result.getFirst().containsKey("id"));
    }

    @Test
    void shouldFilterCustomersByExactAge() {
        List<Map<String, Object>> result = new MapQuery<String, Object>()
                .from(CUSTOMERS_MISSING_DATA)
                .where(ObjectQuery.WhereGroup.WhereCondition.<String>builder()
                        .field("age")
                        .condition(ObjectQuery.WhereGroup.WhereCondition.ConditionType.EQUALS)
                        .value(23).build())
                .execute();

        assertEquals(2, result.size());
    }

    @Test
    void shouldFindCustomersWithNullNames() {
        List<Map<String, Object>> result = new MapQuery<String, Object>()
                .from(CUSTOMERS_MISSING_DATA)
                .where(ObjectQuery.WhereGroup.WhereCondition.<String>builder()
                        .field("name")
                        .condition(ObjectQuery.WhereGroup.WhereCondition.ConditionType.IS_NULL)
                        .build())
                .execute();

        assertEquals(1, result.size());
    }

    @Test
    void shouldSortByAgeDescending() {
        List<Map<String, Object>> result = new MapQuery<String, Object>()
                .from(CUSTOMERS_DATA)
                .sort(List.of(ObjectQuery.Sort.<String>builder().sourceField("age").sortType(ObjectQuery.SortType.DESC).build()))
                .execute();

        assertEquals(31, result.getFirst().get("age"));
    }

    @Test
    void shouldGroupByCityIdAndCountPeople() {
        List<Map<String, Object>> result = new MapQuery<String, Object>()
                .from(CUSTOMERS_MISSING_DATA)
                .where(ObjectQuery.WhereGroup.WhereCondition.<String>builder()
                        .field("cityId")
                        .condition(ObjectQuery.WhereGroup.WhereCondition.ConditionType.IS_NOT_NULL)
                        .build())
                .groupBy("cityId", List.of(
                        ObjectQuery.GroupByAggregation.<Map<String, Object>, String>builder()
                                .aggregationResultField("peopleCount")
                                .aggregationOperation(Collection::size)
                                .build()))
                .execute();

        assertEquals(3, result.size());

        Optional<Map<String, Object>> city1 = result.stream().filter((map) -> map.get("cityId").equals("1")).findFirst();
        Assertions.assertTrue(city1.isPresent());
        assertEquals(1, city1.get().get("peopleCount"));

        Optional<Map<String, Object>> city2 = result.stream().filter((map) -> map.get("cityId").equals("2")).findFirst();
        Assertions.assertTrue(city2.isPresent());
        assertEquals(1, city2.get().get("peopleCount"));

        Optional<Map<String, Object>> city1001 = result.stream().filter((map) -> map.get("cityId").equals("1001")).findFirst();
        Assertions.assertTrue(city1001.isPresent());
        assertEquals(1, city1001.get().get("peopleCount"));
    }

    @Test
    void shouldReturnDistinctRecordsBySelectedField() {
        List<Map<String, Object>> result = new MapQuery<String, Object>()
                .from(CUSTOMERS_MISSING_DATA)
                .select(List.of("cityId"))
                .distinct()
                .execute();

        long uniqueCityIds = result.stream().map(m -> m.get("cityId")).distinct().count();
        assertEquals(uniqueCityIds, result.size());
    }

    @Test
    void shouldReturnOnlySubset() {
        List<Map<String, Object>> result = new MapQuery<String, Object>()
                .from(CUSTOMERS_MISSING_DATA)
                .limit(1, 2)
                .execute();

        assertEquals(2, result.size());
    }

    @Test
    void shouldFindNamesContainingSubstring() {
        List<Map<String, Object>> result = new MapQuery<String, Object>()
                .from(CUSTOMERS_DATA)
                .where(ObjectQuery.WhereGroup.WhereCondition.<String>builder()
                        .field("name")
                        .condition(ObjectQuery.WhereGroup.WhereCondition.ConditionType.CONTAINS)
                        .value("An")
                        .build())
                .execute();

        assertEquals(2, result.size());
    }

    @Test
    void shouldReturnRecordsWithMismatchedJoins() {
        List<Map<String, Object>> result = new MapQuery<>(MapQuery.STRING_KEY_ALIASING_FUNCTION)
                .from(CUSTOMERS_MISSING_DATA, "customer")
                .join(CITIES_DATA, "city", "customer.cityId", "city.id", ObjectQuery.JoinType.FULL_EXCLUSIVE)
                .execute();

        assertTrue(result.stream().anyMatch(r -> r.get("customer.cityId") == null || r.get("city.id") == null));
    }
}