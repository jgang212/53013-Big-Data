3-1:

3-2:
The jgang32 folder contains the Java package minus the jar files.

jgang32/src/main/thrift/WeatherAnalytics.thrift contains the thrift
file for the data.

jgang32/src/main/java/edu/uchicago/mpcs53013/DemoSerialization.java
contains my updated code from the example that deals with the GSOD
data instead.

thrift32.out in the main directory is a thrift output example using
data from 710730-99999-1990.op, which is also in the main directory.

3-3:
The jgang folder contains the Java package minus the jar files.

jgang/src/main/java/edu/uchicago/mpcs53103/jgang/WordCount.java
contains my updated code from the example that now splits words
into trigraphs before counting them.

part-r-00000 in the main directory is an output example of a run on
the following text: "some message blah hello world world"