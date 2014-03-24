package com.twitter.summingbird.example

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.spout.ISpout
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.utils.Time
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.{Tuple, Fields, Values}
import com.twitter.tormenta.spout.Spout
import java.util
import scala.collection.{TraversableOnce, mutable}
//import scala.TraversableOnce

//import java.util._
//import util.Random
import java.util.{Map => JMap, Random}
import collection.mutable.{Map, HashMap}
import util.Random
import util.List
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
//import twitter4j._

//(fn:Tuple3<String> => TraversableOnce[T])


object RandomSentenceSpout
{
//val QUEUE_LIMIT = 1000 // default max queue size.
   val FIELD_NAME = "tweet" // default output field name.
  //def apply(fieldName: String)

  def apply(fieldName: String = FIELD_NAME): RandomSentenceSpout[String] =
    new RandomSentenceSpout(fieldName)(i => Some(i))

  //def apply(fieldName: String):RandomSentenceSpout[String] = new RandomSentenceSpout(fieldName)(i => Some(i))
  //(i => Some(i))
}
//(fn: Tuple => TraversableOnce[T])


class RandomSentenceSpout[+T](fieldName:String)(fn: String => TraversableOnce[T])extends BaseRichSpout with Spout[T] {

  //val sentences = mutable.Queue("tweet"->"the cow jumped over the moon",
  //  "tweet"->"an apple a day keeps the doctor away",
  //  "tweet"->"four score and seven years ago",
  //  "tweet"->"snow white and the seven dwarfs",
  //  "tweet"->"i am at two with nature")

  val limit = 1000 // default max queue size.
  val FIELD_NAME = "tweet" // default output field name.

 //val stream: Tuple3<String> = null
  var collector: SpoutOutputCollector = null
  lazy val queue = new LinkedBlockingQueue[Map[String,String]](limit)
  def AddItem()
  {
    val sentence = Map("tweet"->"Sentence")
    var a:Int = 1
    while(a>0)
    {
      Time.sleep(10)
      queue.offer(sentence)
      //println(sentence)
      a = a+1
   }
  }
 def onException(ex: Exception) {}

 def getSpout = this

 override def open(conf: JMap[_,_], context:TopologyContext, coll: SpoutOutputCollector)
  {
    collector = coll
    AddItem()
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer)
  {
    declarer.declare(new Fields(FIELD_NAME))
  }

  def onEmpty: Unit = Time.sleep(50)

  override def nextTuple()
  {

    //val q0 = collection.immutable.Queue("1","Two","iii")
    println("#####POLLING#######")
    Option(queue.poll) match{
    //  case None => onEmpty
      case Some(items) => items.foreach{item => collector.emit(new Values(item.asInstanceOf[AnyRef]))}
    }
  }
   //   val sentences = List("tweet"->"the cow jumped over the moon",
   //   "tweet"->"an apple a day keeps the doctor away",
   //   "tweet"->"four score and seven years ago",
   //   "tweet"->"snow white and the sfneven dwarfs",
   //  "tweet"->"i am at two with nature")
   //println(sentences)
   //  Time.sleep(100)
   //for( sentence <- sentences ){
   //println("###"+sentence+"####")
   // (1, "hello").productIterator
   //   .foreach {
   //  case s: String => println("string: " + s)
   //  case i: Int => println("int: " + i)
   // }
   // Thread sleep 10
   // collector.emit(new Values(sentence.asInstanceOf[AnyRef]))
   //println("###EMITTING####")
   // collector.emit(sentences(Random.nextInt(sentences.length)))
    //val sentence:Option[String] = null
  //sentence match {
  //  case None => onEmpty
  //  case Some(sentences) => sentences.foreach(sentence => collector.emit(new Values(sentence.asInstanceOf[AnyRef])))
  //    println("###EMITTING####")
  //  }


  // val sentence:Option[] = null
  // sentence match{
    //      case None => onEmpty
      //    case Some(items) => items.foreach{item => collector.emit(new Values(item.asInstanceOf[AnyRef]))}

   //WORKS FROM HERE:
   //Time.sleep(100)
   //val sentence:String = null
   //sentences.foreach(sentences => collector.emit(new Values(sentence.asInstanceOf[AnyRef])))
   // println("###EMITTING####")
   //sentences.productIterator.map{case Some(sentences) => sentences.productIterator.foeach{sentence => collector.emit(new Values(item.asInstanceOf[AnyRef]))

  //   val sentences = ("tweet"->"dsjfka sdfaf","tweet"->"adfjhadfja fdss","tweet"->"fdsafdasfd fdsfd")
 //   Time.sleep(100)
 //    println("PRODUCT ITERATOR")
  //    sentences.productIterator.map{sentence => collector.emit(new Values(sentence.asInstanceOf[AnyRef]))}
    
   //var sentence = sentences(Random.nextInt(sentences.length))
   //collector.emit(new Values(sentence))
override def flatMap[U](newFn: T => TraversableOnce[U]) = new RandomSentenceSpout(fieldName)(fn(_).flatMap(newFn))

}

