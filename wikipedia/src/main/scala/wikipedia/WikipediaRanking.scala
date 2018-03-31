package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

class WikipediaRanking (val masterAddress: String) {
  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setAppName("LanguageRanking")
  if (!masterAddress.isEmpty) {
    conf.setMaster(masterAddress)
  }

  val sc: SparkContext = new SparkContext(conf)
  
  /** Problem 0:
   *  Returns the number of articles on which the language `lang` occurs.
   *  Hint: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
     rdd.filter(x => x.mentionsLanguage(lang) == true).count().toInt    
  }

  /* Problem 1: Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
     langs.map(x => ( x, occurrencesOfLang(x, rdd))).sortBy(x=> x._2).reverse
  }

  /* Problem 2:
   * Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
      val articles = rdd.flatMap(wiki => {
      val access = langs.filter(lang => wiki.mentionsLanguage(lang) == true)
      access.map(lang => (lang, wiki))
    })
    articles.groupByKey
  }

  /* Problem 3: Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
     index.mapValues(x=>x.toArray.length).sortBy(x=>x._2).collect().toList.reverse
  }

  /* Problem 4: Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking?
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
     rdd.flatMap(wiki => {
     langs.filter(lang => wiki.mentionsLanguage(lang) == true).map(x=>(x,1))
     }).reduceByKey(_+_).takeOrdered(15)(Ordering[Int].reverse.on(_._2)).toList
  }

  def stopSC() = sc.stop()
}

object WikipediaRanking extends WikipediaRanking("local")

object WikipediaRunner {
  def main(args: Array[String]) {
    val inputPath = args(0)

    val wikipediaRanking = new WikipediaRanking("")

    val wikiRdd: RDD[WikipediaArticle] = wikipediaRanking.sc.textFile(inputPath).map(WikipediaData.parse)

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", wikipediaRanking.rankLangs(wikipediaRanking.langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = wikipediaRanking.makeIndex(wikipediaRanking.langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", wikipediaRanking.rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", wikipediaRanking.rankLangsReduceByKey(wikipediaRanking.langs, wikiRdd))

    langsRanked3.map(println)

    /* Output the speed of each ranking */
    println(timing)
    wikipediaRanking.stopSC()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
