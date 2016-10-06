package com.lucas.newman

import org.apache.spark.graphx.VertexId

case class Aresta(
            id:VertexId,
            cost:Double
            )
object Aresta{
  def criaAresta(vertex:Long,cost:Double)={
    Aresta(vertex,cost)

  }
}

