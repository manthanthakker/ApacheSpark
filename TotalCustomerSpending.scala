package com.sparkproject.customerdataanalysis

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object totalSpending {
  
  // Function to parse data
  def parseData(x:String):(Int, Double)={
    val record=x.split(",");
    (record(0).toInt,record(2).toDouble)
  }
  
def main(args:Array[String]){
  
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
    
    // Input File
    val input=sc.textFile("/Users/trailbrazer/Desktop/MR/apachespark/SparkScala/customer-orders.csv")
    
    // Parse the data to return (customerId,amountSpent)
    val customerSpending=input.map(parseData)
    
    // Compute total amount spent
    val totalCustomerSpending=customerSpending.reduceByKey((x,y)=>x+y)
    
    // Just Print the final result
    totalCustomerSpending.foreach(println)
    
   
 }
}
