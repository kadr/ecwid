package orm.Db

import com.charleskorn.kaml.Yaml
import kotlinx.serialization.Serializable
import java.io.File
import java.sql.*
import java.util.*
import kotlin.collections.ArrayList


/**
 * Класс для работы с запросами в базу
 *
 * @author Марьев Евгений
 * @property tableName Название таблицы в базе
 * @constructor Читаем данные для подключения к базе из файла my.yml и подкоючаеися к базе.
 * @throws SQLException Логин или пароль не корректные
 * @throws Exception Какая то ошибка
 *
 */
class Connector(private val tableName: String) : DbInterface {
    private var conn: Connection? = null
    private var where: Map<String, Any> = mapOf()
    private var whereOr: Map<String, Any> = mapOf()
    private var whereIn: Map<String, List<Any>> = mapOf()
    private var whereLike: Map<String, Any> = mapOf()
    private var whereLikeOr: Map<String, Any> = mapOf()
    private var orderBy: String = ""
    private var groupBy: String = ""
    private var join: String = ""
    private var limit: String = ""

    private var columns: Array<String> = arrayOf()
    private var from: Array<Source> = arrayOf()
    private var joins: Array<Join> = arrayOf()
    private var whereClauses: String = ""
    private var groupByColumns: Array<String> = arrayOf()
    private var having: String = ""
    private var sortColumns: Array<Sort> = arrayOf()
    private var limitRaw: Int = 100
    private var offset: Int = 0


    data class Source(
            val table: String,
            val alias: String
    )

    data class Join(
            val table: String,
            val condition: Map<String, String>
    )

    data class Sort(
            val field: String,
            val order: String
    )

    @Serializable
    data class DB(
            val host: String,
            val port: Int,
            val name: String,
            val user: String,
            val password: String
    )

    init {
        val input = File("my.yml").readText()
        val config = Yaml.default.parse(DB.serializer(), input)
        val connectionProps = Properties()
        with(connectionProps) {
            put("user", config.user)
            put("password", config.password)
        }
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance()
            conn = DriverManager.getConnection(
                    "jdbc:" + "mysql" + "://" +
                            config.host +
                            ":" + config.port.toString() + "/" +
                            config.name,
                    connectionProps)
        } catch (ex: SQLException) {
            ex.message
        } catch (ex: Exception) {
            ex.message
        }
    }

    fun parseRawSql(sql: String): Connector {
        columns = searchSelectFields(sql)
        from = searchFromTables(sql)
        joins = searchJoinTables(sql)
        whereClauses = searchWhereClauses(sql)
        groupByColumns = searchGroupByColumns(sql)
        having = searchHavingClauses(sql)
        sortColumns = searchOrderByColumns(sql)
        limitRaw = searchLimit(sql)
        offset = searchOffset(sql)

        return this
    }

    /**
     * Ищем поля для оператора SELECT
     * @param sql строка с sql запрсом
     *
     * @return список полей
     */
    private fun searchSelectFields(sql: String): Array<String> {

        val fields: String = Regex(pattern = """(?!SELECT)\s+[\w\.,\s\(\)]+\s+FROM""",
                option = RegexOption.IGNORE_CASE)
                .find(input = sql)?.value.toString()
                .replace("FROM", "")
                .trim()

        return fields.split(",").map { field -> field.trim() }.toTypedArray()
    }

    /**
     * Ищем таблицы для оператора FROM
     * @param sql строка с sql запрсом
     *
     * @return список структур таблица, алиас
     */
    private fun searchFromTables(sql: String): Array<Source> {
        var tables: String = Regex(pattern = """FROM\s+[\w\.,\s\(\)]+\s(LEFT|RIGHT|INNER|WHERE|JOIN|ORDER|GROUP|LIMIT|OFFSET)""",
                option = RegexOption.IGNORE_CASE)
                .find(input = sql)?.value.toString()
                .replace("FROM", "")
                .trim()
        for (tr in arrayOf("LEFT", "RIGHT", "INNER", "WHERE", "JOIN", "ORDER", "GROUP", "LIMIT", "OFFSET")) {
            tables = tables.toUpperCase().replace(tr, "")
        }
        tables = tables.toLowerCase()
        var from: ArrayList<Source> = arrayListOf()
        for (t in tables.split(",")) {
            val split = t.trim().split("""\s""")
            if (split.size > 1) {
                from.add(Source(table = split[0].trim(), alias = split[1].trim()))
            } else {
                from.add(Source(table = split[0].trim(), alias = ""))
            }
        }

        return from.toTypedArray()
    }

    /**
     * Ищем связи для оператора JOIN
     * @param sql строка с sql запрсом
     *
     * @return список структур таблица, условие
     */
    private fun searchJoinTables(sql: String): Array<Join> {
        var joins: String = Regex(pattern = """(LEFT|RIGHT|INNER)\s+JOIN\s+[\w\.,\s\(\)=]+\s(WHERE|ORDER|GROUP|LIMIT|OFFSET)""",
                option = RegexOption.IGNORE_CASE)
                .find(input = sql)?.value.toString()
                .replace("JOIN", "")
                .trim()
        for (tr in arrayOf("LEFT", "RIGHT", "INNER", "WHERE", "ORDER", "GROUP", "LIMIT", "OFFSET")) {
            joins = joins.toUpperCase().replace(tr, "")
        }
        joins = joins.toLowerCase()
        var join: ArrayList<Join> = arrayListOf()
        val split = joins.trim().split("on")
        val cond = split[1]
                .trim()
                .replace("(", "")
                .replace(")", "")
                .split("=")
        join.add(Join(table = split[0].trim(), condition = mapOf(cond[0].trim() to cond[1].trim())))

        return join.toTypedArray()
    }

    /**
     * Ищем условия для фильтрации
     * @param sql строка с sql запрсом
     *
     * @return строка с условиями
     */
    private fun searchWhereClauses(sql: String): String {
        var clause: String = Regex(pattern = """WHERE\s+[\w\.,\s\(\)=\'><]+\s*(ORDER|GROUP|LIMIT|OFFSET)""",
                option = RegexOption.IGNORE_CASE)
                .find(input = sql)?.value.toString()
                .replace("WHERE", "")
                .trim()
        for (tr in arrayOf("ORDER", "GROUP", "LIMIT", "OFFSET", "order", "group", "limit", "offset")) {
            clause = clause.replace(tr, "").trim()
        }

        return clause.trim()
    }

    /**
     * Ищем поля для группипрвки GROUP BY
     * @param sql строка с sql запрсом
     *
     * @return список полей
     */
    private fun searchGroupByColumns(sql: String): Array<String> {
        var group: String = Regex(pattern = """GROUP BY\s+[\w\.,\s]+\s(HAVING|ORDER|LIMIT|OFFSET)""",
                option = RegexOption.IGNORE_CASE)
                .find(input = sql)?.value.toString()
                .replace("GROUP BY", "")
                .trim()
        for (tr in arrayOf("HAVING", "ORDER", "LIMIT", "OFFSET")) {
            group = group.replace(tr, "")
        }

        return group.trim().split(",").map { f -> f.trim() }.toTypedArray()
    }

    /**
     * Ищем условия для фильтрации после группировки
     * @param sql строка с sql запрсом
     *
     * @return строка с условиями
     */
    private fun searchHavingClauses(sql: String): String {

        var havingClause = if (sql.contains("ORDER", true)) {
            Regex(pattern = """HAVING\s+[\w\(\)\*<>\s\.,]+ORDER""",
                    option = RegexOption.IGNORE_CASE)
                    .find(input = sql)?.value.toString()
                    .replace("HAVING", "")
                    .trim()
        } else {
            Regex(pattern = """HAVING\s+[\w\(\)\*<>\s\.,]+LIMIT""",
                    option = RegexOption.IGNORE_CASE)
                    .find(input = sql)?.value.toString()
                    .replace("HAVING", "")
                    .trim()
        }

        for (tr in arrayOf("ORDER", "LIMIT", "OFFSET", "order", "limit", "offset")) {
            havingClause = havingClause.replace(tr, "").trim()
        }

        return havingClause.trim()
    }

    /**
     * Ищем поля для сортировки ORDER BY
     * @param sql строка с sql запрсом
     *
     * @return список полей
     */
    private fun searchOrderByColumns(sql: String): Array<Sort> {
        var sort = Regex(pattern = """ORDER BY\s+[\w\.,\s]+\s(HAVING|GROUP|LIMIT|OFFSET)""",
                option = RegexOption.IGNORE_CASE)
                .find(input = sql)?.value.toString()
                .replace("ORDER BY", "")
                .trim()
        for (tr in arrayOf("HAVING", "GROUP", "LIMIT", "OFFSET")) {
            sort = sort.replace(tr, "")
        }
        var list: ArrayList<Sort> = arrayListOf()
        for (split in sort.trim().split(",")) {
            val split = split.trim().split(" ")
            list.add(Sort(split[0], split[1]))
        }

        return list.toTypedArray()
    }

    /**
     * Ищем LIMIT
     * @param sql строка с sql запрсом
     *
     * @return число
     */
    private fun searchLimit(sql: String): Int {
        var limit = Regex(pattern = """LIMIT\s+[\d,]+\s*""",
                option = RegexOption.IGNORE_CASE)
                .find(input = sql)?.value.toString()
                .replace("LIMIT", "")
                .trim()
        for (tr in arrayOf("OFFSET")) {
            limit = limit.replace(tr, "")
        }
        if (limit.contains(",")) {
            return limit.split(",")[1].toInt()
        }

        return limit.toInt()
    }

    /**
     * Ищем OFFSET
     * @param sql строка с sql запрсом
     *
     * @return число
     */
    private fun searchOffset(sql: String): Int {
        var offset: String = if (sql.contains("OFFSET", true)) {
            Regex(pattern = """OFFSET\s+\d+\s*""",
                    option = RegexOption.IGNORE_CASE)
                    .find(input = sql)?.value.toString()
                    .replace("OFFSET", "")
                    .trim()
        } else {
            Regex(pattern = """LIMIT\s+[\d,]+\s*""",
                    option = RegexOption.IGNORE_CASE)
                    .find(input = sql)?.value.toString()
                    .replace("LIMIT", "")
                    .trim()

        }
        if (offset.contains(",", true)) {
            offset = offset.split(",")[0]
        }

        return offset.toInt()
    }

    /**
     * Геттер для полей SELECT
     *
     * @return список полей
     */
    fun getSelectFields(): Array<String> {
        return columns
    }

    /**
     * Геттер для табличек
     *
     * @return список таблиц
     */
    fun getFromTables(): Array<Source> {
        return from
    }

    /**
     * Геттер для связей
     *
     * @return список связей
     */
    fun getJoinTables(): Array<Join> {
        return joins
    }

    /**
     * Геттер для WHERE
     *
     * @return строка с условиями
     */
    fun getWhereClause(): String {
        return whereClauses
    }

    /**
     * Геттер для полей GROUP BY
     *
     * @return список полей
     */
    fun getGroupByFields(): Array<String> {
        return groupByColumns
    }

    /**
     * Геттер для HAVING
     *
     * @return строка с условиями
     */
    fun getHavingClause(): String {
        return having
    }

    /**
     * Геттер для полей ORDER BY
     *
     * @return список полей
     */
    fun getOrderByFields(): Array<Sort> {
        return sortColumns
    }

    /**
     * Геттер для LIMIT
     *
     * @return число
     */
    fun getLimit(): Int {
        return limitRaw
    }

    /**
     * Геттер для OFFSET
     *
     * @return число
     */
    fun getOffset(): Int {
        return offset
    }

    /**
     * Получить записи из базы
     * @param fields мапа с полями и значениями для выборки
     * @throws SQLException ошибка при выполнении запроса
     *
     * @return результат выборки
     */
    override fun get(fields: List<String>, tableAlias: String): ResultSet {
        var stmt: Statement? = null
        val sql: String = "SELECT ${fields.joinToString(", ")} " +
                "FROM $tableName $tableAlias " +
                join +
                prepareWhere() +
                orderBy +
                groupBy +
                limit

        var result: ResultSet?
        try {
            stmt = conn!!.createStatement()
            result = stmt!!.executeQuery(sql)
            if (result.fetchSize > 0) {
                return result
            }
        } catch (ex: SQLException) {
            throw SQLException(ex.message)
        }

        return result
    }

    /**
     * Добавляем новую запись
     * @param fields мапа с полями и значениями для добавления
     * @throws SQLException ошибка при выполнении запроса
     *
     * @return Количество обновленных строк
     */
    override fun insert(fields: Map<String, Any>): Int {
        var stmt: Statement? = null
        val sql: String = "INSERT INTO $tableName " +
                "(${fields.keys.joinToString(", ")}) " +
                "VALUES (${fields.values.joinToString(", ")})"
        var affectedRows = 0
        try {
            stmt = conn!!.createStatement()
            affectedRows = stmt!!.executeUpdate(sql)
            if (affectedRows > 0) {
                return affectedRows
            }
        } catch (ex: SQLException) {
            println(ex.message)
            return affectedRows
        }
        return affectedRows
    }

    /**
     * Обновляем запись
     * @param fields мапа с полями и значениями для обновления
     * @throws SQLException ошибка при выполнении запроса
     *
     * @return Количество добавленных строк
     */
    override fun update(fields: Map<String, Any>): Int {
        var stmt: Statement? = null
        var affectedRows = 0
        var updateFields: ArrayList<String> = arrayListOf()

        for ((key, value) in fields) {
            updateFields.add("$key = $value")
        }

        if (where.isEmpty()) {
            throw java.lang.Exception("where is not present. It can't be empty")
        }
        val sql = "UPDATE $tableName " +
                "SET ${updateFields.joinToString(", ")} " +
                prepareWhere()
        try {
            stmt = conn!!.createStatement()
            affectedRows = stmt!!.executeUpdate(sql)
            if (affectedRows > 0) {
                return affectedRows
            }
        } catch (ex: SQLException) {
            println(ex.message)
            return affectedRows
        }
        return affectedRows
    }

    /**
     * Удаляем запись
     * @throws SQLException ошибка при выполнении запроса
     *
     * @return Количество добавленных строк
     */
    override fun delete(): Int {
        var stmt: Statement? = null
        var affectedRows = 0

        var sql = "DELETE FROM $tableName ${prepareWhere()} "

        try {
            stmt = conn!!.createStatement()
            affectedRows = stmt!!.executeUpdate(sql)
            if (affectedRows > 0) {
                return affectedRows
            }
        } catch (ex: SQLException) {
            println(ex.message)
            return affectedRows
        }
        return affectedRows
    }

    /**
     * Сетер для условия where с объединением через AND
     * @param filter
     *
     * @return this
     */
    fun where(filter: Map<String, Any>): Connector {
        where = filter

        return this
    }

    /**
     * Сетер для условия where с объединением через OR
     * @param filter
     *
     * @return this
     */
    fun whereOr(filter: Map<String, Any>): Connector {
        whereOr = filter

        return this
    }

    /**
     * Сетер для условия where с объединением через IN
     * @param filter
     *
     * @return this
     */
    fun whereIn(filter: Map<String, List<Any>>): Connector {
        whereIn = filter

        return this
    }

    /**
     * Сетер для условия where like с объединением через AND
     * @param filter
     *
     * @return this
     */
    fun whereLike(filter: Map<String, Any>): Connector {
        whereLike = filter

        return this
    }

    /**
     * Сетер для условия where like с объединением через OR
     * @param filter
     *
     * @return this
     */
    fun whereLikeOr(filter: Map<String, Any>): Connector {
        whereLikeOr = filter

        return this
    }

    /**
     * Устанавливаем ограничение на выборку
     * @param n число возвращаемых записей
     *
     * @return this
     */
    fun limit(n: Int): Connector {
        limit = " LIMIT $n "

        return this
    }

    /**
     * Задаем сортировку
     * @param fields поля для сортировки
     *
     * @return this
     */
    fun orderBy(fields: Map<String, Any>): Connector {
        val w: ArrayList<String> = arrayListOf()
        for ((key, value) in fields) {
            w.add("$key $value")
        }
        if (w.isNotEmpty()) {
            orderBy = " ORDER BY ${w.joinToString(", ")} "
        }

        return this
    }

    /**
     * Задаем группировку
     * @param fields поля для группировки
     *
     * @return this
     */
    fun groupBy(fields: List<String>): Connector {
        if (fields.isNotEmpty()) {
            groupBy = " GROUP BY ${fields.joinToString(", ")} "
        }

        return this
    }

    /**
     * Задаем связи
     * @param joinTable таблица для связи, с условиями привязки
     *
     * @return this
     */
    fun innerJoin(joinTable: Map<String, Map<String, String>>): Connector {
        if (joinTable.isNotEmpty()) {
            for ((table, conditions) in joinTable) {
                join += " INNER JOIN $table ON "
                var j: ArrayList<String> = arrayListOf()
                for ((parentTableCond, joinTableCond) in conditions) {
                    j.add("$parentTableCond = $joinTableCond")
                }
                join += j.joinToString(" AND ")
            }
        }

        return this
    }

    /**
     * Подготавливаем where для запроса
     *
     * @return строку where для SQL запроса
     */
    private fun prepareWhere(): String {
        var result = " "
        if (where.isNotEmpty()) {
            val w: ArrayList<String> = arrayListOf()
            for ((key, value) in where) {
                w.add("$key = $value")
            }
            result += "${w.joinToString(" AND ")} "
        }
        if (whereOr.isNotEmpty()) {
            val w: ArrayList<String> = arrayListOf()
            for ((key, value) in whereOr) {
                w.add("$key = $value")
            }
            result += "${w.joinToString(" OR ")} "
        }
        if (whereIn.isNotEmpty()) {
            for ((field, list) in whereIn) {
                result += "${field} in (${list.joinToString(",")}) "
            }
        }

        if (whereLike.isNotEmpty()) {
            val w: ArrayList<String> = arrayListOf()
            for ((key, value) in whereLike) {
                w.add("$key LIKE $value")
            }
            result += "${w.joinToString(" AND ")} "
        }

        if (whereLikeOr.isNotEmpty()) {
            val w: ArrayList<String> = arrayListOf()
            for ((key, value) in whereLikeOr) {
                w.add("$key LIKE $value")
            }
            result += "${w.joinToString(" OR ")} "
        }

        return if (result.isNotBlank()) {
            " WHERE $result "
        } else {
            result
        }
    }

}
