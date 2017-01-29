/** 
 * To Run: spark-shell -i spark-connector101.scala \
 *                     [--conf "spark.mongodb.input.uri=mongodb://<USERNAME>:<PASSWORD>@<HOST:PORT>/<INPUT_DB>.<INPUT_COLLECTION>"]
 *                     [--conf "spark.mongodb.output.uri=mongodb://<USERNAME>:<PASSWORD>@<HOST:PORT>/<OUTPUT_DB>.<OUTPUT_COLLECTION>"] 
 *                     --packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0
 *
 * Understanding the package versioning: 
 *     - mongo-spark-connector_2.11 indicates that we are using the spark connector with Scala version 2.11
 *     - :2.0.0 indicates that we are using version 2.0.0 of Spark.
 *
 * Data: media.mongodb.org/zips.json
 */

/**
 * First, lets load the required libraries for using the MongoDB-Spark connector
 */
import com.mongodb.spark._
import com.mongodb.spark.config._

/**
 * By default the spark.mongodb.input.uri config paramater will determine the the input collection.
 * However, it is possible for us to create new ReadConfig objects anytime we want to dynamically change the 
 * collection we wish to use as an input source e.g. lets read from the zipcode collection in our test database.
 */
val readConfig = ReadConfig("test","zips", Option("mongodb://127.0.0.1:27017"))
val zipsRDD = MongoSpark.load(sc, readConfig)

/** 
 * Using the aggregation framework in MongoDB we can pre-filter and pre-aggregate our data before performing 
 * subsequent analysis in Spark. e.g. here is how we can extract only zipcodes with a population greater 
 * than 5,000 people 
 */
import org.bson.Document;
val populatedZipsRDD = zipsRDD.withPipeline(Seq(Document.parse("{ $match : { pop : { $gt : 5000 } } }")))

/** 
 * As you will know, all transformation in Spark are lazily executed. This means no computation or data transfer
 * has occured up to this point in the script. Only when we execute an action command will any computation be
 * run and data pulled from our MongoDB instance. Here are some examples of action commands:
 */
populatedZipsRDD.cache()
populatedZipsRDD.collect()
populatedZipsRDD.take(10)
populatedZipsRDD.first()

/**
 * Using our raw dataset (zippsRDD) we could perform MapReduce operations no this data or we can convert it
 * to a DataFrame which is a highly optomised version of a RDD and perfrom some aggregations using raw Spark
 * commands. For example, here is a pipeline of operations that sort in descending order an aggregated count 
 * of all state that contain zipcodes with greater than 25,000 inhabitants:
 */
val zipsPerStateDF = (zipsRDD.toDF()
                      .filter("pop > 25000")
                      .groupBy("state")
                      .count()
                      .sort(desc("count")))

/**
 * The power of the MongoDB Spark conncetor reveals itself in this instance because we can perform this 
 * computation all locally from within our MongoDB instance natively using the aggregation pipeline:
 */

val zipsPerStateMongoRDD = zipsRDD.withPipeline( 
    Seq( Document.parse("{ $match : { pop : { $gt : 25000 } } }"),
         Document.parse("{ $group : { _id: \"$state\", count: { \"$sum\": 1 } } }"),
         Document.parse("{ $sort : {count : -1} }")
    ) )

/**
 * We can also run SparkSQL queries on our dataset. The first thing we need to do is import the relevant libraries
 * and instantiate a sparkSession so that we can apply SQL queries to our dataset. Then we need to create 
 * a temporary view of our RDD. The best way to achieve this is with the createOrReplaceTempView function. 
 * Then, using the sparkSession object we can call the sql function passing a SQL query as a string
 */
import org.apache.spark.sql.{SQLContext, SparkSession}

val sparkSession = SparkSession.builder().getOrCreate()

zipsRDD.toDF().createOrReplaceTempView("zipcode")
val zipsPerStateSqlDF = sparkSession.sql("SELECT first(state) as state, count(zip) as count FROM zipcode WHERE pop > 25000 GROUP BY state ORDER BY count DESC")
zipsPerStateSqlDF.show()

/**
 * Now lets save our aggregated result back into MongoDB
 */
val writeConfig = WriteConfig("test", "spark101", Option("mongodb://127.0.0.1:27017"))
MongoSpark.save(zipsPerStateDF, writeConfig)

