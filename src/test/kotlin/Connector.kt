import org.junit.Ignore
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import orm.Db.Connector

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class ConnectorTest {
    private val catalog: Connector = Connector(tableName = "Product_product")
    private val fields = listOf<String>(
            "id",
            "active",
            "model",
            "product_type",
            "image",
            "position"
    )
    private val insert = mapOf<String, Any>(
            "active" to true,
            "model" to "'Model'",
            "product_type" to "'Lumix'",
            "image" to "'link'",
            "position" to 1,
            "image_front" to "'image'",
            "image_back" to "'image'",
            "is_lumix_pro" to false,
            "is_test_drive" to false,
            "href" to "'href'",
            "created_at" to "'2019-01-01'",
            "serial_number" to "'asdsdfsa3243123'",
            "serial_number_check" to "'asdsdfsa3243123'",
            "brand" to "'brand'"
    )
    private val update = mapOf<String, Any>(
            "active" to false,
            "model" to "'Model new'",
            "product_type" to "'Canon'",
            "image" to "'link new'",
            "position" to 2
    )
    private val sql = "SELECT author.name, count(book.id), sum(book.cost) " +
            "FROM author, post, book " +
            "LEFT JOIN book b ON (b.author_id = a.id) " +
            "WHERE id > 3 AND name = 'James' OR title = 'July' AND age < 25" +
            "GROUP BY author.name, book.title " +
            "HAVING COUNT(*) > 1 AND SUM(book.cost) > 500 ORDER BY id DESC, title ASC LIMIT 10 OFFSET 5"


    @Test
    @Order(1)
    @DisplayName("Should add 1 record")
    fun shouldAddOneRecord() {
        val affectedRows = catalog.insert(insert)
        assertEquals(1, affectedRows)
    }

    @Test
    @DisplayName("Should update 1 record")
    @Order(2)
    fun shouldUpdateOneRecord() {
        val affectedRows = catalog
                .where(mapOf("model" to "'Model'"))
                .update(update)
        assertEquals(1, affectedRows)
    }

    @Test
    @DisplayName("Should return not null list of records")
    @Order(3)
    fun shouldReturnNotNullListOfRecords() {
        val result = catalog.get(fields)
        assertNotNull(result)
    }

    @Test
    @DisplayName("Should return list of records")
    @Order(4)
    fun shouldReturnListOfRecords() {
        val result = catalog.get(fields)
        assertEquals("JDBC42ResultSet", result::class.simpleName)
    }

    @Test
    @DisplayName("Should return list of records with filter where")
    @Order(5)
    fun shouldReturnListOfRecordsWithFilterWhere() {
        val result = catalog
                .where(mapOf("model" to "'Model new'", "position" to 2))
                .get(fields)
        assertNotNull(result)
    }

    @Test
    @DisplayName("Should return list of records with filter whereOr")
    @Order(6)
    fun shouldReturnListOfRecordsWithFilterWhereOr() {
        val result = catalog
                .whereOr(mapOf("model" to "'Model new'", "position" to 2))
                .get(fields)
        assertNotNull(result)
    }

    @Test
    @DisplayName("Should return list of records with filter whereIn")
    @Order(7)
    fun shouldReturnListOfRecordsWithFilterWhereIn() {
        val result = catalog
                .whereIn(mapOf("model" to listOf("'Model new'")))
                .get(fields)
        assertNotNull(result)
    }

    @Test
    @DisplayName("Should return list of records with filter whereLike with AND")
    @Order(8)
    fun shouldReturnListOfRecordsWithFilterWhereLike() {
        val result = catalog
                .whereLike(mapOf("model" to "'Model name'", "image" to "'link new'"))
                .get(fields)
        assertNotNull(result)
    }

    @Test
    @DisplayName("Should return list of records with filter whereLike with OR")
    @Order(9)
    fun shouldReturnListOfRecordsWithFilterWhereLikeOr() {
        val result = catalog
                .whereLikeOr(mapOf("model" to "'Model new'", "image" to "'link new'"))
                .get(fields)
        assertNotNull(result)
    }

    @Test
    @DisplayName("Should return list of records with order")
    @Order(10)
    fun shouldReturnListOfRecordsOrdered() {
        val result = catalog
                .orderBy(mapOf("model" to "DESC", "id" to "ASC"))
                .get(fields)
        assertNotNull(result)
    }

    @Test
    @DisplayName("Should return list of records with limit")
    @Order(11)
    fun shouldReturnListOfRecordsLimit() {
        val result = catalog
                .limit(1)
                .get(fields)
        assertNotNull(result)
    }

    @Test
    @DisplayName("Should return list of records with group")
    @Order(12)
    fun shouldReturnListOfRecordsGroup() {
        val result = catalog
                .groupBy(listOf("model"))
                .get(fields)
        assertNotNull(result)
    }

    @Test
    @DisplayName("Should return list of records with join table")
    @Order(13)
    fun shouldReturnListOfRecordsJoin() {
        val result = catalog
                .innerJoin(mapOf("ProductRatio_productratio pr" to mapOf("p.id" to "pr.product_id")))
                .get(listOf<String>(
                        "p.id",
                        "p.active",
                        "p.model",
                        "p.product_type",
                        "p.image",
                        "p.position"
                ), "p")
        assertNotNull(result)
    }

    @Test
    @DisplayName("Should delete 1 record")
    @Order(14)
    fun shouldDeleteOneRecord() {
        val affectedRows = catalog
                .where(mapOf("model" to "'Model new'"))
                .delete()
        assertEquals(1, affectedRows)
    }

    @Test
    @DisplayName("Should take SELECT fields from sql raw string and return it")
    @Order(15)
    fun shouldTakeSelectFieldsFromSqlRawString() {
        val select = arrayOf("author.name", "count(book.id)", "sum(book.cost)")

        assertArrayEquals(select, catalog.parseRawSql(sql).getSelectFields())
    }

    @Test
    @DisplayName("Should take FROM tables from sql raw string and return it")
    @Order(16)
    fun shouldTakeFromTablesFromSqlRawString() {
        val tables = arrayOf(
                Connector.Source("author", ""),
                Connector.Source("post", ""),
                Connector.Source("book", "")
        )

        assertArrayEquals(tables, catalog.parseRawSql(sql).getFromTables())
    }

    @Test
    @DisplayName("Should take JOIN tables from sql raw string and return it")
    @Order(17)
    fun shouldTakeJoinTablesFromSqlRawString() {
        val joins = arrayOf(
                Connector.Join("book b", mapOf("b.author_id" to "a.id"))
        )

        assertArrayEquals(joins, catalog.parseRawSql(sql).getJoinTables())
    }

    @Test
    @DisplayName("Should take WHERE clause from sql raw string and return it")
    @Order(18)
    fun shouldTakeWhereClauseFromSqlRawString() {
        val where = "id > 3 AND name = 'James' OR title = 'July' AND age < 25"

        assertEquals(where, catalog.parseRawSql(sql).getWhereClause())
    }

    @Test
    @DisplayName("Should take GROUP BY fields from sql raw string and return it")
    @Order(19)
    fun shouldTakeGroupByFieldsFromSqlRawString() {
        val group = arrayOf("author.name", "book.title")

        assertArrayEquals(group, catalog.parseRawSql(sql).getGroupByFields())
    }

    @Test
    @DisplayName("Should take ORDER BY fields from sql raw string and return it")
    @Order(20)
    fun shouldTakeOrderByFieldsFromSqlRawString() {
        val order = arrayOf(Connector.Sort("id", "DESC"),
                Connector.Sort("title", "ASC"))

        assertArrayEquals(order, catalog.parseRawSql(sql).getOrderByFields())
    }

    @Test
    @DisplayName("Should take LIMIT fields from sql raw string and return it")
    @Order(21)
    fun shouldTakeLimitFromSqlRawString() {
        val limit = 10

        assertEquals(limit, catalog.parseRawSql(sql).getLimit())
    }

    @Test
    @DisplayName("Should take OFFSET fields from sql raw string and return it")
    @Order(22)
    fun shouldTakeOffsetFromSqlRawString() {
        val offset = 5

        assertEquals(offset, catalog.parseRawSql(sql).getOffset())
    }

    @Test
    @DisplayName("Should take HAVING clause from sql raw string and return it")
    @Order(23)
    fun shouldTakeHavingFromSqlRawString() {
        val having = "COUNT(*) > 1 AND SUM(book.cost) > 500"

        assertEquals(having, catalog.parseRawSql(sql).getHavingClause())
    }

}
