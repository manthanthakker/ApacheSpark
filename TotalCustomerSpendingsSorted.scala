package com.sparkproject.movierating

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
    
    // Flip the key value
    val customerSpendingCustomer=totalCustomerSpending.map((customerIdTotalSpending)=>(customerIdTotalSpending._2,customerIdTotalSpending._1))
    
    // sort the records by totalSpending in desceding order
    val customerSpendingSorted = customerSpendingCustomer.sortByKey(false)
    
    // Flip the key value
    val customerId_Spending_Sorted= customerSpendingSorted.map(amountCustomerId=>(amountCustomerId._2,amountCustomerId._1))
    
    // Just Print the final result
    customerId_Spending_Sorted.collect().foreach(println)
    
 }
}
