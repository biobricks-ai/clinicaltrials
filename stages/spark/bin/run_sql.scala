import org.apache.iceberg.spark.CommitMetadata
import scala.collection.JavaConverters._

val sqlFile = sys.env.getOrElse("SQL_FILE", {
  System.err.println("Please set SQL_FILE environment variable")
  System.exit(1)
  ""
})

// Get optional commit properties from environment
val commitPropertiesJson = sys.env.get("COMMIT_PROPERTIES")

// Parse JSON commit properties if provided
val commitProperties: Map[String, String] = commitPropertiesJson match {
  case Some(json) =>
    try {
      // Use Spark's built-in JSON parsing
      import org.apache.spark.sql.functions._
      import spark.implicits._

      val jsonDF = spark.read.json(Seq(json).toDS)
      val row = jsonDF.first()

      // Convert Row to Map[String, String]
      jsonDF.schema.fields.map { field =>
        val value = row.getAs[Any](field.name)
        (field.name, value.toString)
      }.toMap
    } catch {
      case e: Exception =>
        System.err.println(s"Error parsing COMMIT_PROPERTIES: ${e.getMessage}")
        Map.empty[String, String]
    }
  case None => Map.empty[String, String]
}

val source = scala.io.Source.fromFile(sqlFile)
val sqlContent = source.mkString
source.close()

// Split on /^-- @@$/ to be very strict (line must contain only "-- @@")
val sqlStatements = sqlContent
  .split("(?m)^-- @@$")
  .map(_.trim)
  .filter(_.nonEmpty)

// Function to execute SQL with optional commit properties
def executeSql(stmt: String): Unit = {
  println(s">>> Running SQL:\n${stmt.take(120)}...")

  if (commitProperties.nonEmpty && (stmt.toUpperCase.contains("MERGE INTO") ||
                                    stmt.toUpperCase.contains("INSERT") ||
                                    stmt.toUpperCase.contains("UPDATE") ||
                                    stmt.toUpperCase.contains("DELETE"))) {
    println(s">>> Adding commit properties: $commitProperties")

    try {
      // Set commit properties using the withCommitProperties method
      CommitMetadata.withCommitProperties(
        commitProperties.asJava,
        new java.util.concurrent.Callable[Void] {
          def call(): Void = {
            val result = spark.sql(stmt)
            result.show()
            null
          }
        },
        classOf[RuntimeException]
      )
    } catch {
      case e: Exception =>
        System.err.println(s"Error executing SQL with commit properties: ${e.getMessage}")
        throw e
    }
  } else {
    val result = spark.sql(stmt)
    result.show()
  }
}

// Execute all SQL statements
try {
  for (stmt <- sqlStatements) {
    executeSql(stmt)
  }
} catch {
  case e: Exception =>
    System.err.println(s"Script failed with error: ${e.getMessage}")
    e.printStackTrace()
    System.exit(1)
}

System.exit(0)
