val sqlFile = sys.env.getOrElse("SQL_FILE", {
  System.err.println("Please set SQL_FILE environment variable")
  System.exit(1)
  ""
})

val source = scala.io.Source.fromFile(sqlFile)
val sqlContent = source.mkString
source.close()

// Split on /^-- @@$/ to be very strict (line must contain only "-- @@")
val sqlStatements = sqlContent
  .split("(?m)^-- @@$")
  .map(_.trim)
  .filter(_.nonEmpty)

for (stmt <- sqlStatements) {
  println(s">>> Running SQL:\n${stmt.take(120)}...")
  spark.sql(stmt)
}

System.exit(0)
