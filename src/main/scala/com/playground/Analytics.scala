package com.playground

import org.apache.spark.sql.SparkSession
import java.io._
import org.apache.spark.sql.functions.col

import org.apache.log4j.Logger
import org.apache.log4j.Level


object Analytics {
  
  val location = "file:///c:/Users/gurub/tickers/output/output.csv"
  
  def main(args : Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    val symbol = if ( args.length > 0 ) args(0) else ""
    
    val spark = SparkSession
                  .builder
                  .appName("stock analytics")
                  .master("local")
                  .getOrCreate
    
    spark.sparkContext.setLogLevel("DEBUG")
    
    val rdd = spark.read.csv(location)
    
    import spark.implicits._

    val df = rdd.toDF("SYMBOL","SERIES","OPEN","HIGH","LOW","CLOSE","LAST","PREVCLOSE","TOTTRDQTY","TOTTRDVAL","TIMESTAMP","TOTALTRADES","ISIN","JUNK")
              .drop("JUNK")
              .filter(col("SERIES") === "EQ")
    
    //open a ledger file for writing 
//    val bw = new BufferedWriter(new FileWriter("ledger.txt"))
    
    val count = df.count
    println(" there are " + count + " records ") 
    
    df.show
    
    df.createOrReplaceTempView("tickers")
    
    val subject = spark.sql("select * from tickers where symbol like \"%" + symbol  + "%\" order by from_unixtime(unix_timestamp(timestamp,'dd-MMM-yyyy'),'yyyy-MM-dd') ")
    
    subject.show(100)

  }

}
