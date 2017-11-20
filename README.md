8-1

I've added my partial work to cgi-bin/, html/ and 
SparkStreaming.scala.

8-2

Since Bloom filters can never have false negatives (only false
positives), it work wells for our scenario. If a customer doesn't 
have special delivery instructions but we look it up anyway, it just 
wastes a bit of time and doesn't affect the delivery much. On the 
other hand, false negatives are unacceptable here because if a 
customer has special delivery instructions, we can't ignore that.

To reduce the number of database lookups for ordinary customers 
without special delivery instructions, I would set up the Bloom 
filter as follows. 

Depending on how much space we have for the hash table, we can 
determine how many bits (m) to allocate in our database. In this 
case, the number of special delivery customers is our n. I would 
then use m / n * (ln 2) hash functions to minimize the number of 
false positives (call this number of hash functions h). Then, for 
each customer with special delivery instructions (not ordinary 
customers), we would hash some identifier (customer id, for example)
with all of these hash functions to fill up h bits in our hash table
for each not ordinary customer.

Then when we go through our entire customer list (ordinary and not)
for delivery, we would run each customer identifier across our h hash
functions to see if those h bits occur in our hash table. If they do,
then that customer "probably" has special delivery instructions and
we should check for it during the delivery.