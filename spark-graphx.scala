/** 
 * To Run: spark-shell -i spark-graphx.scala \
 *                     [--conf "spark.mongodb.input.uri=mongodb://<USERNAME>:<PASSWORD>@<HOST:PORT>/<INPUT_DB>.<INPUT_COLLECTION>"]
 *                     [--conf "spark.mongodb.output.uri=mongodb://<USERNAME>:<PASSWORD>@<HOST:PORT>/<OUTPUT_DB>.<OUTPUT_COLLECTION>"] 
 *                     --packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0
 *
 * Understanding the package versioning: 
 *     - mongo-spark-connector_2.11 indicates that we are using the spark connector with Scala version 2.11
 *     - :2.0.0 indicates that we are using version 2.0.0 of Spark.
 *
 * Data: 
 */

/**
 * First, lets load the required libraries for using the MongoDB-Spark connector
 */
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.bson.Document
import org.apache.spark.graphx._
import org.apache.spark.sql.Row

/**
 * Lets load our MongoDB data into an RDD
 */
val readConfig = ReadConfig("air","routes", Option("mongodb://127.0.0.1:27017"))
val routesRDD = MongoSpark.load(sc, readConfig)

/*
 * Convert our RDD into an "edges" RDD in order to create a graph
 */
val edgesDF = (routesRDD
                .toDF()
                .selectExpr("airline['name'] AS airline", "src_airport AS src", "dst_airport AS dst") )
val edges =  edgesDF.rdd.map { case Row(long_src: Long, long_dst: Long, attr: Row) => Edge(long_src, long_dst, attr) }

/**
 * Create a VertexRDD of distinct airports 
 */
val readConfig = ReadConfig("air","airports", Option("mongodb://127.0.0.1:27017"))
val airportRDD = MongoSpark.load(sc, readConfig)
val vertices = (airportRDD
                  .toDF()
                  .withColumn("id", monotonically_increasing_id())
                  .select("id", "airport")
                  .rdd.map { case Row(long_id: Long, attr: Row) => (long_id, attr) } )

/**
 * Now create a graph based on our defined vertices and edges
 */
val graph = Graph(vertices, edges)

/**
 * Now we can run an array of graph algorithms on our data set including PageRank, Connected Components and Triangle Counting
 */
val ranks = graph.pageRank(0.0001).vertices
val connComp = graph.connectedComponents().vertices
val triCounts = graph.triangleCount().vertices

/**
 * We can join our results back to the vertices for reference
 */
vertices.toDF().cache()
val ranksByAirport = vertices.join(ranks)
val connCompByAirport = vertices.join(connComp)
val triCountsByAirport = vertices.join(triCounts)

