3-1:

The jgang31 folder contains the Java package minus the jar files.

jgang31/src/main/java/edu/uchicago/mpcs53103/jgang31/WordCount.java
contains my updated code that includes the combiner function.

On the cluster, running regular WordCount on gutenberg/books.txt
takes:
Total time spent by all maps in occupied slots (ms)=606746
Total time spent by all reduces in occupied slots (ms)=481496
Total time spent by all map tasks (ms)=303373
Total time spent by all reduce tasks (ms)=120374

With the version that uses combiners, it takes:
Total time spent by all maps in occupied slots (ms)=531864
Total time spent by all reduces in occupied slots (ms)=231788
Total time spent by all map tasks (ms)=265932
Total time spent by all reduce tasks (ms)=57947

As we can see from above, the WordCount that uses combiners takes
noticeably less time to run than the version without. This is because
in the first verison of WordCount, almost all the words get sent over
the network, which violates the guiding principle of Hadoop that is
to bring the code to the data. In the second version, combiner
functions summarize the map's output records, and this combiner
output is instead sent over the network to limit the volume of data
transfer. Combiners make sense with large data sets since they will
replace the set of original map outputs with fewer/smaller outputs.

-------------------------------

3-2:

The jgang32 folder contains the Java package minus the jar files.

jgang32/src/main/thrift/WeatherAnalytics.thrift contains the thrift
file for the data.

jgang32/src/main/java/edu/uchicago/mpcs53013/DemoSerialization.java
contains my updated code from the example that deals with the GSOD
data instead.

thrift32.out in the main directory is a thrift output example using
data from 710730-99999-1990.op, which is also in the main directory.

-------------------------------

3-3:

The jgang folder contains the Java package minus the jar files.

jgang/src/main/java/edu/uchicago/mpcs53103/jgang/WordCount.java
contains my updated code from the example that now splits words
into trigraphs before counting them.

part-r-00000 in the main directory is an output example of a run on
the following text: "some message blah hello world world"