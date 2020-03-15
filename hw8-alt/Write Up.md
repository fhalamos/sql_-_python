# Course reflection
Felipe Alamos

## 1) What technical things (e.g. ssh, VMs, etc) you wish you knew at the start of the quarter or how you wish technical concepts would have been taught/reinforced at the start of the class;

This answer might not be very fair because I had some background in CS. Its hard for me to know how would I felt with the course without that background. I generally feel its always good to help everyone feel comfortable with:

- Git version control. Specifically for working with branches, managing conflicts. 
- Managing environments
- Good coding practices

It seems to me that the technical background de 121 and 122 provides are appropiate for this course (ex, general data structures, hashing, good coding skills). I did not take those courses though so also cannot comment too much on that.

## 2) What technical concepts do you feel like you still do not have a good grip on at this point in the quarter

* Writing efficient sql. We mentioned that in the query processing process the optimizer should take care of making querys efficient, and that sql is declarative language more than imperative. Nevertheless, I can imagine that different ways of writing querys should also influence performance significantly? I felt we could have talked more about "good sql practices". For example, is it ok to "abuse" on the use of common table expressions? When should they be preferred over doing joins, or is it just the same?

* Indices. Im still not sure if I fully understand when to create them, which type, how many, and the tradeoffs involved. I would say the following, please correct me:
- B+ tree index: Good for ranks. Ex, if im going to be making queries such as "give me everyone whose age is over 5", a b+ tree index on age could be good. Is this also true in the case of an unclustered index? I feel we would need to do a random i/o for each record, which would be bad... why would we ever create unclustered index? Sometimes we just dont have an option due to how data is stored? How do we define how data is stored in disc (so as to know if we are generating a clustered or unclustered index)? 
- Hash index: Good for quick look up of single value. We only need to hash value, and then we will get the address of the record. Ex, find record based on email. In which cases a b+ tree index would be prefered for these lookups? 
- Why do primary keys are implemented with B+ trees index by default instead of Hash index?
-How does the system know if to use and index or scan a whole file?

* When studying access methods, I felt the discussion was a bit too theoric and hard to connect it with implementations. For example, we studied plenty of join methods. I think that was cool for the sake of understanding how things work in the back, but what takeaways should we take once we are implementing a system?

* Its sometimes hard to grasp the connection and how to complement Data Structures and Data Bases. Say we want to work with a Red Black Tree to store our information, due to its balances properties. Those trees would be completely in memory, right? Is there any chance to map them to the DBs and keep its properties?


## 3) What database concepts you wish we covered or covered more (or if you feel none, what concepts did you enjoy the most).

* I think the last part of the course (nosql dbs) was too fast and bit hard to grip. It seemed very theoretical but hard to understand implementations. Maybe having a homework where we had to work with  nosql dbs, and compare tradeoffs with sql, would have been useful.

* It would have also been cool to learn about different sql engines and how to choose between them. We did everything on postgres.

* I really enjoyed everything related to data integration. I felt thats super useful and cool skill to have.

* Related to parallel distributed DBs, could have been great to do some implementations of that too. 