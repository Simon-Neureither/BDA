package search

package object types {
  type Word = String
  type DocumentTitle = String
  type SearchResult = (DocumentTitle, Double)
}

import search.types.{DocumentTitle, SearchResult, Word}

import scala.collection.mutable.ListBuffer

/**
 Zuordnung Word zu Anzahl der Instanzen
 */
case class WordVector(private val sentence: Traversable[Word],
                      documentTitle: Option[DocumentTitle]) extends Traversable[(Word, Int)] {
  private val wordCount: Map[Word, Int] = sentence.groupBy(identity).mapValues(_.size)
  
  val totalWordCount: Int = wordCount map (_._2) sum

  def countOccurrencesOf(word: Word): Int = wordCount.getOrElse(word, 0)

  override def foreach[U](f: ((Word, Int)) => U): Unit = wordCount.foreach(f)
}

case class TfidfVector(private val data: Map[Word, Double],
                       documentTitle: Option[DocumentTitle]) {
  val length: Double = Math.sqrt(data.values.map(a => a * a).sum)

  def apply(word: Word): Double = data.getOrElse(word, 0)

  def commonWordsWith(other: TfidfVector) = data.keySet.intersect(other.data.keySet)
}

class IndexedDocuments(documentsSpace: Seq[WordVector],
                       docsContainingWord: Map[Word, Set[DocumentTitle]]
                        ) {
  val docCount = documentsSpace.size
  
  val documentsAsTfidfSpace: Seq[TfidfVector] = documentsSpace map wordVectorToTfIdfVector toSeq

  def wordVectorToTfIdfVector(wordVector: WordVector): TfidfVector = {
    val data: Map[Word, Double] = wordVector map {
      case (word, _) => (word, tfidf(word, wordVector))
    } toMap

    TfidfVector(data, wordVector.documentTitle)
  }

  def tfidf(word: Word, wordVector: WordVector): Double = {
    if (documentsSpace.isEmpty) {
      0
    }
    else {
      val occurrencesInDoc: Double = wordVector.countOccurrencesOf(word).toDouble
      val tf = occurrencesInDoc / wordVector.totalWordCount
      val numDocsContainingWord = docsContainingWord.getOrElse(word, Seq.empty).size
      if (numDocsContainingWord == 0) {
        0
      } else {
        val idf = docCount / numDocsContainingWord.toDouble
        val tfidf = tf * Math.log(idf)
        tfidf
      }
    }
  }
  
//Ähnlichkeitssuche
def compareWithQuery(vectorFromUser: TfidfVector)(vectorFromCorpus: TfidfVector): (DocumentTitle, Double) = {
    val vec = vectorFromCorpus

    val commonWords = vectorFromUser.commonWordsWith(vec)
    val numerator = commonWords map (word => vectorFromUser(word) * vec(word)) sum
    val denominator = vectorFromUser.length * vec.length
    (vec.documentTitle.get, numerator / denominator)
  }

//Normalisieren
def search(sentence: Traversable[Word], topN: Int): Seq[SearchResult] = {
    require(topN > 0, s"Top N has to be greater than 0 but was $topN")

    val queryTfidfVector = wordVectorToTfIdfVector(WordVector(sentence, None))
    val scoredDocuments: Seq[(DocumentTitle, Double)] = documentsAsTfidfSpace.map(compareWithQuery(queryTfidfVector))
    scoredDocuments
      .sortWith(_._2 > _._2) // Absteigende Punktzahl
      .filterNot(_._2.isNaN)
      .filterNot(_._2 < 0.000001)
      .take(topN)
  }

}

object Indexer {

  def indexDocuments(documents: Iterator[(DocumentTitle, List[Word])]): IndexedDocuments = {
    import scala.collection.mutable

    val documentsSpace = new ListBuffer[WordVector]
    val docsContainingWord = new mutable.HashMap[Word, Set[DocumentTitle]]

    documents.foreach {
      case (title, words) =>
        documentsSpace += WordVector(words, Some(title))
        words.foreach { word =>
          val currentDocsWithThisWord = docsContainingWord.getOrElse(word, Set.empty)
          docsContainingWord += word -> (currentDocsWithThisWord + title)
        }
    }

    new IndexedDocuments(
      documentsSpace.toVector,
      docsContainingWord.toMap
    )
  }

}

package search

import search.types.{DocumentTitle, Word}

object SearchApp extends App {
  require(args.length > 0, "Usage: As an argument you should pass the path to the folder containing documents (program will search through subdirectories")

  val topNResults: Int = 10

  val extractWords: (String) => Seq[String] = s => s.split("\\W+").map(_.toLowerCase).filterNot(_.trim.isEmpty)

  val documentLoader: Iterator[(DocumentTitle, List[Word])] = {
    import java.io.File
    def recursiveListFiles(f: File): Array[File] = {
      // Subdirectory in Scala
      val these = f.listFiles
      these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
    }

    val files = recursiveListFiles(new File(args.head)).filter(_.isFile)
    println(s"Found ${files.length} documents")

    files zip Stream.from(1) map { case (f, i) =>
      if (i % 200 == 0) println(s"Loaded $i documents so far")
      (f.getName, io.Source.fromFile(f).getLines().flatMap(extractWords).toList)
    } toIterator

    // test data
    //    List(
    //      ("doc1", List("welcome", "to", "scala", "labs")),
    //      ("doc2", List("welcome", "to", "Toronto")),
    //      ("doc3", List("introduce", "scala", "and", "enjoy", "scala")),
    //      ("doc4", List("hello", "scala"))
    //    )
  }

  println("Indexing, please wait...")
  val indexedDocuments = Indexer.indexDocuments(documentLoader)
  println("Indexing... DONE")

  while (true) {
    println()
    println("Enter a sentence or 'q' to quit")
    val input: String = Console.in.readLine()
    if (input == "q")
      System.exit(0)
    println(s"Querying...")
    val topMatches = indexedDocuments.search(extractWords(input), topNResults)
    if (topMatches.isEmpty)
      println("Couldn't find any relevant documents")
    else {
      println(s"Top results:")
      topMatches.foreach(println)
    }
  }
}


