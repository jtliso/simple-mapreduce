# Programming Assignment 2 UTK COSC 560
### J.T. Liso and Sean Whalen

#
## Part 1: Identifying and removing stop words

For this part we updated the Java code for word count linked in the project writeup. We added word clean-up such as removing whitespace, removing beginning and trailing punctuation, and removing any characters that did not fit the ASCII space.

In order to identify stop words, we added a check to the reducer to only add words that are above a certain threshold frequency. We began with a high frequency of 5000 and decreased the value until we added a word to the stop word list that was deemed meaningful. We noticed that if we dropped the threshold below 1000, we would lose an important word 'queen', so we decided to keep the stop word threshold frequency at 1000.

These words and their counts can be seen in results/stopwords.txt.

### Usage:

```
hadoop fs -put ../txt/shakespeare.txt /input
hadoop com.sun.tools.javac.Main WordCount.java 
jar cf wc.jar WordCount*.class
hadoop jar wc.jar WordCount /input /stopwords
```
