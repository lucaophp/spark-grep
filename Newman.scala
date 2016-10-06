package com.lucas.newman

import org.apache.spark.{SparkConf, SparkContext}

object Newman{
  val sc=new SparkContext(new SparkConf().setAppName("SparkGrep").setMaster("local[*]"))
  var grafo:Grafo=new Grafo(sc,"/home/lucas/Downloads/testdata.txt",delimiter = " ")
  def main(args:Array[String]):Unit={

    val g=grafo
    g.run()
  }
}

