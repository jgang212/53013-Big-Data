6-1:

Original WordCount:
val counts = sawyer.flatMap(line => line.split(" ")).
map(word => (word, 1)).
reduceByKey{case (x, y) => x + y};

Using countByValue:
val counts = sawyer.flatMap(line => line.split(" ")).countByValue();

While it is faster, countByValue returns a dictionary that can take
up a lot of memory, so for larger texts, reduceByKey may be more
efficient from a space perspective.

6-2:
We want to first ignore punctuation and make all the words lowercase
so we get a more accurate (though not perfect) answer to this
question:

val counts = sawyer.map(line => line.replaceAll("[,.!?:;-]", "")).
map(line => line.toLowerCase).
flatMap(line => line.split(" ")).
map(word => (word, 1)).
reduceByKey{case (x, y) => x + y}

The following then returns the average count per word:

counts.values.sum() / counts.keys.count()

