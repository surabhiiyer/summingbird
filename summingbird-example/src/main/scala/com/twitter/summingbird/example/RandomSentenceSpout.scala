package com.twitter.summingbird.example

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.spout.ISpout
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.utils.Time
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.{Tuple, Fields, Values}
import com.twitter.tormenta.spout.Spout
//import java.util._
//import util.Random
import java.util.{Map => JMap, Random}
//import java.util.concurrent.LinkedBlockingQueue
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

  //val QUEUE_LIMIT = 1000 // default max queue size.
  val FIELD_NAME = "tweet" // default output field name.

 // val stream: Tuple3<String> = null 
  var collector: SpoutOutputCollector = null
  
  //lazy val queue = new LinkedBlockingQueue[String](limit)
  //var s: String
  //lazy val listner =  queue.offer(s)

 def onException(ex: Exception) {}

 def getSpout = this

  override def open(conf: JMap[_,_], context:TopologyContext, coll: SpoutOutputCollector)
  {
    collector = coll
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer)
  {
    declarer.declare(new Fields(FIELD_NAME))
  }

  def onEmpty: Unit = Time.sleep(50)

  override def nextTuple()
  {

    var sentence:String = null
 //   Option(queue.poll).map(fn)match{
 //     case None => onEmpty
 //     case Some(items) => itmes.foreach{item => collector.emit(new Values(item.asInstanceOf[AnyRef]))}
    val sentences = ("tweet"->"dsjfka sdfaf","tweet"->"adfjhadfja fdss","tweet"->"fdsafdasfd fdsfd")
    Time.sleep(100)
    sentences.productIterator.map{sentence => collector.emit(new Values(sentence.asInstanceOf[AnyRef]))}
    
   //var sentence = sentences(Random.nextInt(sentences.length))
   //collector.emit(new Values(sentence))
 }
override def flatMap[U](newFn: T => TraversableOnce[U]) = new RandomSentenceSpout(fieldName)(fn(_).flatMap(newFn))
}

