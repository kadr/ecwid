package orm.Db

import java.sql.ResultSet

interface DbInterface {
    fun get(fields: List<String>, tableAlias: String = ""): ResultSet
    fun insert(fields: Map<String, Any>): Int
    fun update(fields: Map<String, Any>): Int
    fun delete(): Int
}