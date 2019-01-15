import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import spark.implicits._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

val session = SparkSession.builder().appName("test").master("spark://xxxxx:7077")

val df = spark.read.format("org.elasticsearch.spark.sql").option("es.nodes.wan.only","true").option("es.port","9200").option("es.nodes", "es-server").option("es.index.auto.create", "true")

********How to load and write the parquet ****************
val result = df.option("es.read.field.as.array.include","fields.tags , beat , tags").load("logstash-haproxy-2018.12.30/haproxy")
result.write.parquet("/home/spark/data/logstash-haproxy-2018.12.30.parquet")

********How to read the parquet file back into spark****************

val parquetFileDF = spark.read.parquet("/home/spark/data/logstash-haproxy-2018.12.30.parquet")





*****kmeans******

val data = spark.sql("select time_duration,time_backend_response from logs where message like '%error%'")

val parsedata = data.rdd.map{
     case (row) => val features = Array[Double](row.getString(0).toDouble,row.getString(1).toDouble,row.getString(3).toDouble)
     Vectors.dense(features)
    }
val model = KMeans.train(parsedata,3,20)
