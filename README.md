## BigData Platform Spark Task - handle wikipedia dataset

In this task, you will get to know Spark by exploring full-text Wikipedia articles. You are going to use full-text data from Wikipedia to produce a rudimentary metric of how popular a programming language is by counting the number of occurances of each languages in a Wikipedia article. 

### Task 0
* Download the target input file from the below link [http://alaska.epfl.ch/~dockermoocs/bigdata/wikipedia.dat](http://alaska.epfl.ch/~dockermoocs/bigdata/wikipedia.dat)
* After downloading the file, upload the file to the HDFS, and capture the screenshot of `hdfs dfs -ls FILE_PATH` below
![image here](https://github.com/leeky-courses-2017-02/bigdata-project-wikipedia-sungho123/blob/master/task0.png)

### Task 1
You have to implement 5 functions to count the number of occurances of candidate languages. They are `occurrencesOfLang, rankLangs, makeIndex, rankLangsUsingIndex, rankLangsReduceByKey` in `wikipedia/src/main/scala/wikipedia/WikipediaRanking.scala` 
* Implement `occurrencesOfLang` - Start by implementing a helper method which computes the number of articles in an `RDD` of type `RDD[WikipediaArticles]` that mention the given language at least once. For the sake of simplicity we check that it least one word (delimited by spaces) of the article text is equal to the given language.
* Implement `rankLangs` - Using `occurrencesOfLang` implement `rankLangs` that computes a list of pairs where the second component of the pair is the number of articles that mention the language (the first component of the pair is the name of the language). An example of what might return might look like this, for example: List(("Scala",999999),("JavaScript",1278),("LOLCODE",982),("Java",42)). The list should be sorted in descending order.
* Implement `makeIndex` - Compute an inverted index. An inverted index is an index data structure storing a mapping from content, such as words or numbers, to a set of documents. In particular, the purpose of an inverted index is to allow fast full text searches. In our use-case, an inverted index would be useful for mapping from the names of programming languages to the collection of Wikipedia articles that mention the name at least once. To make working with the dataset more efficient and more convenient, implement a method that computes an inverted index which maps programming language names to the Wikipedia articles on which they occur at least once. This function returns an RDD of the following type: `RDD[(String, Iterable(WikipediaArticles)]`. This RDD contains pairs, such that for each language in the given `langs` list there is at most one pair. Furthermore, the second component of each pair (the `Iterable`) contains the `WikipediaArticles` that mention the language at least once. HINT: Hint: You might want to use methods `flatMap` and `groupByKey` on `RDD` for this part
* Implement `rankLangsUsingIndex` - Using `makeIndex` output, implement a faster method for computing the language ranking. Like in `rankLangs` implementation, `rankLangsUsingIndex` should compute a list of pairs where the second component of the pair is the number of articles that mention the language (the first component of the pair is the name of the language). Again, the list should be sorted in descending order. HINT: `mapValues` on `pairRDD` could be useful for this part.
* Implement `rankLangsReduceByKey` - In the case where the inverted index from above is only used for computing the ranking and for no other task, it is more efficient to use the method `reduceByKey` to compute the ranking directly, without first computing an inverted index. Note that the `reduceByKey` method is only defined for RDDs containing pairs (each pair is interpreted as a key-value pair). Implement the method, this time computing the ranking without the inverted index, using `reduceByKey`. Again, the list should be sorted in descending order.

After implementing all the above methods, commit them to your own repository.

### Task 2
In the root folder (under `wikipedia`), run `sbt test` to confirm the compile works and your implemented methods pass the unit test cases. Capture the outcome that says it passes all the test, and embed it below. Note that the captured image should contain the hostname of your test machines.
![Image here](https://github.com/leeky-courses-2017-02/bigdata-project-wikipedia-sungho123/blob/master/task2.png)

### Task 3
Build a jar file to submit it in the spark cluster by issuing `sbt package`. The output jar file will be located in `wikipedia/target/scala-2.11/homework-2_2.11-0.1-SNAPSHOT.jar` . Using the file issue `spark-submit` command - `spark-submit --class wikipedia.WikipediaRunner --master spark://node-1:7077 JAR_FILE_LOCATION WIKIPEDIA_INPUT_DATASET_LOCATION` .  Capture the outcome from the execution that contains Language and count list with the running time of each method. Note that the captured image should contain the hostname of your test machines.
![Image here](https://github.com/leeky-courses-2017-02/bigdata-project-wikipedia-sungho123/blob/master/task3_!.png)
![Image here](https://github.com/leeky-courses-2017-02/bigdata-project-wikipedia-sungho123/blob/master/task3.png)

### Task 4
Write a report why the three methods - `rankLangs, rankLangsUsingIndex, rankLangsReduceByKey` - of counting shows different performaces. To help you to dig into Spark detail, you can refer articles from web - [Google search result](https://goo.gl/eFzMcm). In the report, you have to write how you interpret and analyze the result by using Spark WebUI - screen capture is highly recommended.

Ack. The contents and source codes are referenced from [coursera](https://www.coursera.org/learn/scala-spark-big-data/home/welcome)
