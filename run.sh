chmod 700 src/wordcount_mapper.py
chmod 700 src/wordcount_reducer.py

export HADOOP_HOME=/usr/local/hadoop-2.7.3
export PATH=$PATH:/usr/local/hadoop-2.7.3/bin

`hadoop jar /usr/local/hadoop-2.7.3/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -files src/wordcount_mapper.py,src/wordcount_reducer.py -mapper src/wordcount_mapper.py -reducer src/wordcount_reducer.py -input /test/shakespeare.txt -output /test/output.txt`


