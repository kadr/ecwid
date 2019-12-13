package orm

import orm.Db.Connector


fun main(arg: Array<String>) {
    val catalog = Connector(tableName = "Catalog_catalog")

    val sql = "SELECT author.name, count(book.id), sum(book.cost) FROM author, post, book LEFT JOIN book b ON (b.author_id = a.id) WHERE id = 3 AND name = 'James' GROUP BY author.name HAVING COUNT(*) > 1 AND SUM(book.cost) > 500 ORDER BY id LIMIT 10,2"

    println(catalog.parseRawSql(sql))

//    (id = 3 AND name = 'James') OR (id=5 AND name = 'Kate')
}