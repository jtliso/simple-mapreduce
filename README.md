# Programming Assignment 2 UTK COSC 560
### J.T. Liso and Sean Whalen

## Setup
On Cloudlab setup is as follows:
```
export HADOOP_HOME=/usr/local/hadoop-2.7.3
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
export PATH=${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=<path-to-current-director>/tools.jar
```

Jar is not installed on CloudLab for whatever reason so you need to install fastjar.
```
sudo apt-get install fastjar
```

### Preprocessing
We need to append the line numbers to the files to do the inverted indexing.
```
python preprocess.py ../txt/shakespeare.txt
python preprocess.py ../txt/frankenstein.txt
```

#
## Part 1: Identifying and removing stop words

For this part we updated the Java code for word count linked in the project writeup. We added word clean-up such as removing whitespace, removing beginning and trailing punctuation, and removing any characters that did not fit the ASCII space.

In order to identify stop words, we added a check to the reducer to only add words that are above a certain threshold frequency. We began with a high frequency of 5000 and decreased the value until we added a word to the stop word list that was deemed meaningful. We noticed that if we dropped the threshold below 1000, we would lose an important word 'queen', so we decided to keep the stop word threshold frequency at 1000.

These words and their counts can be seen in results/stopwords.txt.

### Usage:

```
cd src
sudo -s $HADOOP_HOME/bin/hadoop fs -put ../txt/shakespeare.txt /input/shakespeare
sudo -s $HADOOP_HOME/bin/hadoop fs -put ../txt/frankenstein.txt /input/frankenstein
hadoop com.sun.tools.javac.Main WordCount.java 
fastjar cf wc.jar WordCount*.class
sudo -s $HADOOP_HOME/bin/hadoop jar wc.jar WordCount /input /stopwords
```

#
## Part 2: Creating the Inverted Index

### Usage:

```
hadoop com.sun.tools.javac.Main InvertIndex.java 
fastjar cf invert.jar InvertIndex*.class
sudo -s $HADOOP_HOME/bin/hadoop jar invert.jar InvertIndex /input /inverted
```
