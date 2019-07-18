package com.spotify.dbeam.args

object TestSqlStatements {
  
  val vanillaSelectWithWhere = "SELECT * FROM some_table WHERE column_id > 100"
  val coffeesSelectWithWhere = "SELECT * FROM COFFEES WHERE SIZE > 10"
}
