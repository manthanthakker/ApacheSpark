package com.sparkproject.movierating

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source._

import java.nio.charset.CodingErrorAction
import scala.io.Codec



object mostPopularMovie {
  
  
  // Broadcast Variables
  def intializeMovies():Map[Int,String]={
    
    var movies:Map[Int,String]=Map()
    
   // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    
    val lines=scala.io.Source.fromFile("/Users/trailbrazer/Desktop/MR/apachespark/ml-100k/u.item").getLines()
    
    
    for(line<-lines){
        
      var record=line.split('|')
      if(record.length>2){
         movies+=(record(0).toInt->record(1))
      }
    }
    return movies
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
    
    val movies=sc.broadcast(intializeMovies)
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../ml-100k/u.data")
    
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => (x.toString().split("\t")(1),1))
    
    // Add the counts by MovieId
    val movieRatingsCount=ratings.reduceByKey((x,y)=>(x+y))
    
    // Flip and Sort
    val sortedPopularMovie=movieRatingsCount.map(record=>(record._2,record._1)).sortByKey()
    
    // Flip and get the movie Name from broadcast variable
    val mostPopularMovie=sortedPopularMovie.top(1).map(record=>(movies.value(record._2.toInt),record._1))
    
    
   //Print the most popular movie
    mostPopularMovie.foreach(println)
    
  }
  
}
