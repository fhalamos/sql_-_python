## HW 4 Query and Indexing

Submit your answers in the text file answers.txt. Please put your answer right after the question. If you want to put down how you got to your answer, put it on the line following lines. You can have just the answer,
but there is no chance for partial credit. Assume worst case for all lookups.
If there are multiple ways to look up the data (i.e. multiple indexes, index vs scan), use the cheapest method.

For example, imagine question 0 is ```How long (ms) to read two sequential pages```, your answers.txt would have something like

```
0) 11ms
9+2(1) for one seek and two reads
```

You will need to calculate the B+Tree fanout/degree, by calculating the max number of pointers as m for a B+page:
`page_size >= (m-1) * size(search key) + m * size(pointer)`

Imagine we have a page size of 1kb, with a search key size of 40 bytes and a pointer size of 10, we would have:
`1000 >= (m-1)*40+m*10`
`1040/50>= m`
`20.8>= m`
Since m must be an integer and we would like to hold the largest valid value for m, we therefor know that each page could hold at most 20 pointers and 19 keys.
(e.g  `20*10+19*40<=1000`)

For the following problems you will assume the following
information:

- Page sizes are 4kb (4000 bytes)
- 1mb = 1000kb
- Integers (int) are 4 bytes
- BigInts (big) are 8 bytes
- Characters (char) are 1 bytes
- Strings are fixed length char arrays of size n (str[n])
- Page pointers and pointers to a record ID are 8 bytes
- Assume data and index pages have 0 bytes for header data (magic). Assume
each page is filled up to be as full as possible, and that a record is never split across multiple pages.
- Each data record has 0 bytes for header and null/variable length data (magic)
- Assume each index entry is the search key + record id pointer. No additional metadata.
- Page seek cost is 9ms, page read time is 1ms
- No page is in memory. For questions 4-6 do not assume any page read will stay in memory.
- Hashtables are in memory (but the pages containing the index entries are on disk). Hashtables are static and perfectly sized (no overflow or chaining)
- Blocks = pages = B+Tree node size
- We have the schema `student` composed of :
  ```
  sname str(20) unique
  year int
  sid big
  major int
  ```
  
- We have the schema `course` composed of:
 ```
 cname str(40) unique
 cid int
 major int
 ```

- We have the schema `takes` composed of:
 ```
 cid int
 sid big
 grade int
 ```

- We have 12,000 students, 300 courses, and 34,000 takes records.
- The following indexes exist:
  - `student` has an unclustered hash index on sname, a clustered B+ tree index on sid, and an unclustered B+tree index on year  
  - `course` has an unclustered B+tree index on cname and an unclustered B+tree index on major.
  - `takes` has an clustered B+tree index on sid
- For any join algorithm that uses nested-loop or hashing, you should make the smaller relation either the inner relation or the build relation.

## Questions

1) How big (in kb or mb) are the total data pages (e.g. student, course, + takes)

2a) What are the max search key values per page/node for sid index on takes?

2b) What are the min search key values per page/node  for sid index on takes (assuming you want as dense pages as possible) ?

3) How tall (root to leaf node) is the index (worst case) for the sid index on takes?

4) How long (ms) will it take to find a student's record by sname?

5) How long (ms) will it take to find all students whose year is 2020. Assume we have no more than 1,100 students with this year.

6) How long (ms) will it take to find all courses with major = 134. Assume there are no more than 50 courses per major.  

7) If only 2 pages can be held in memory, how many pages will be read for a block nested loop join between takes and courses with takes as the outer and courses as inner (given in takes:X and courses:Y, do not worry about seeks). 

8) If 151 pages could be held in memory, what join algorithm would be best (fewest pages read and written) for equi-joining takes and courses? How many pages (in terms of takes and courses) would this join algorithm read (given in takes:X and courses:Y, do not worry about seeks)? 

9) Briefly describe how you would extend a simple-hash join to support a left outer join (e.g. outer join for the probe side)

