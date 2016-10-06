package com.lucas.newman
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.centrality.kBC.KBetweenness

class Grafo(private val sc: SparkContext,val csvPath:String,val digraph:Boolean=false,val delimiter:String=";"){
  var g:Graph[Int, Double]=_
  this.read()
  def read()={
    val txt=sc.textFile(csvPath)
    val delimiter=this.delimiter
    val data:RDD[Array[String]]=txt.map(_.split(delimiter))
    val arestas=data.map(x=>{(x(0),x(1))})
    var edges:RDD[Edge[Double]]=arestas.flatMap({line=>
      Array(Edge(line._1.toInt,line._2.toInt,Double.PositiveInfinity))
    })
    if (!digraph){
      edges=arestas.flatMap({line=>
        Array(Edge(line._1.toInt,line._2.toInt,Double.PositiveInfinity),Edge(line._2.toInt,line._1.toInt,Double.PositiveInfinity))
      })
    }

    val graph=Graph.fromEdges(edges,defaultValue = 1)
    this.g=graph
    this
  }






  def getModularity(graph:Graph[Double,Double]):Double={
    val edgesNum=graph.numEdges.toDouble
    val edgesCounts=graph.triplets.flatMap(triplet=>{
      if(triplet.srcAttr==triplet.dstAttr){
        Iterator((triplet.srcAttr,(1,0)))
      }else{
        Iterator((triplet.srcAttr,(0,1)))
      }
    })
    edgesCounts.foreach(println)
    val soma=edgesCounts.groupByKey().map[Double]{
      case (communityId,data)=>
        val (edgesFull,edgesSome)=data.reduce[(Int,Int)]{
          case ((e1,a1),(e2,a2))=>(e1+e2,a1+a2)
        }
        (edgesFull/edgesNum)-Math.pow(edgesSome/edgesNum,2)
    }.sum()
    soma

  }
  def run()={

    var grafo=calculeBetweenness(g,true)
    var max=grafo.vertices.map(_._2).max
    var mod=0.00
    var nmod=0.01
    var converge=false
    var iteracao=0
    var maxIteracao=10
    while(nmod > mod&iteracao<maxIteracao){

        mod=getModularity(grafo)
        grafo=grafo.subgraph((triplet)=>{!(triplet.toTuple._1._2==max&triplet.toTuple._2._2==max)}).cache()


        nmod=getModularity(grafo)
        grafo=calculeBetweenness(grafo)
    }
    grafo.triplets.foreach(println)





  }
  def calculeBetweenness(grafo:Graph[Int,Double],init:Boolean): Graph[Double, Double] ={
    val k=KBetweenness.run(grafo,2)
    k
  }
  def calculeBetweenness(grafo:Graph[Double,Double])={
    val k=KBetweenness.run(grafo,2)
    k
  }

}

