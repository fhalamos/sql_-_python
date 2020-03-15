# Homework Yelp SQL



## Introduction

For this assignment you will be writing SQL and will be using a PostgreSQL server, which provides a standards-compliant SQL implementation.  
We are using version 9.3 of Postgres, so the documentation at https://www.postgresql.org/docs/9.3/static/ may be useful as well.

You will need access to a psql client that supports version 10 or later. On debian/ubuntu machines this is as simple as running
`sudo apt install postgresql-client-common postgresql-client-10`

You will connect to Postgres to use a preloaded database using:

`psql -d yelp -U dbstudent -h addison.cs.uchicago.edu`

when it asks you for the password use *dbrocks33*  
Note that you can add a password to avoid having to type in the password everytime
https://www.postgresql.org/docs/10.6/static/libpq-pgpass.html


## Dataset
We will use a slightly modified version of the Yelp dataset.
https://www.yelp.com/dataset/documentation/main


Once connected, there are two kinds of commands useful to a database user. The first kind are the psql client meta-commands
and the second are SQL statements (which end in a ;).
The most important command is \?, which gives you help on meta-commands. There are two others you will find very useful.  

First, you can list the tables in the database with \dt

```
yelp=# \d
          List of relations
 Schema |   Name   | Type  |  Owner   
--------+----------+-------+----------
 public | business | table | postgres
 public | review   | table | postgres
 public | tip      | table | postgres
 public | users    | table | postgres



```

Second, you can view the schema  of a given table with \d *table_name*

```
yelp=# \d review
              Table "public.review"
    Column    |         Type          | Modifiers
--------------+-----------------------+-----------
 review_id    | character varying(30) | not null
 business_id  | character varying(30) |
 user_id      | character varying(30) |
 stars        | smallint              |
 review_date  | date                  |
 review_text  | text                  |
 useful_votes | integer               |
 funny_votes  | integer               |
 cool_votes   | integer               |
Indexes:
    "review_pkey" PRIMARY KEY, btree (review_id)

```

The second class of useful commands are SQL commands. All SQL queries in PostgreSQL must be terminated with a semi-colon.
For example, to get a list of 10 records in the `users` table, you would type:

`SELECT * FROM users LIMIT 10;`

This query  requests a max 10 rows from the table. Using **LIMIT**  in this manner one can explore the data small bits at a time. If you really wanted to produce all the records, though, the query is:

`SELECT * FROM user;`

You can use Ctrl+C to end a query that is taking too long (it is very possible to write such bad queries even unintentionally).
Note that using the LIMIT keyword by itself offers no guarantee on which 10 rows from the result are
returned, so do not assume an ordering. \\


Finally, you can change the way psql displays the result sets to suit you better.
In particular, wrapped lines in `less` can make the output of wide tables hard to read. If this bothers you, you can try exiting the client
(you can use Ctrl+D) and starting it again with the LESS  env. variable set like this:

` LESS='-S' psql`



### Notes

If you are trying to add a predicate for a character attribute, you must wrap the value in a single quote (e.g name = 'Bob').

### OS X

In years past one student gave the following advice for connecting from OS X. I cannot vouch for it's accuracy or safety -- *so use at your own risk*!



https://eggerapps.at/postico/


Here is how you'd connect to this online database.



Given this from the assignment...



`psql -d yelp -U dbstudent -h addison.cs.uchicago.edu`

You'd connect by opening Postico and selecting "New Favorite, entering in

```
host: addison.cs.uchicago.edu
user: dbstudent
password: dbrocks33
database: yelp
```



Depending on how you connected, you might get a warning that it can't verify this host. I ignore that in this case or use SSL :)

After connecting you can click on the tables and examine them quickly. To run a query, just hit "SQL Query" in the left sidebar and write some valid sql. Your queries will be saved, so you can click the back button below the text box.


Note: All queries must end in ";" for the assignment, but Postico might allow you to execute the query without it. Just be sure to double check.


### Submission
You will submit the homework in your git repository via chisubmit for assignment hw3.

You have to submit 1 file for each item in a problem:  your query in SQL
(the file extension should be .sql).


All the .sql  files for your answers are already creates, so you just need to insert your answers.
*No output is needed*


## Problems
Schemas for output as provided. Note that the tables are incomplete (e.g. reviews without corresponding businesses or users). 
In problems below we specify to only consider users that exist in their respective tables (e.g. do not count a 
review if there is no corresponding user for the review).  **However, count reviews that do not have corresponding businesses tuples.**
*Note that queries running longer than 300 seconds will terminate*


### 1
Find the 10 users with the most reviews. Only consider reviews that have a corresponding user tuple. In case of ties, break with name
ascending (e.g. bob before sarah).  (given as user_id, name, review_count);

### 2
For the 10 users from the prior question, calculate the average stars of all their reviews.
 Use a nested subquery to calculate the  result. (given as avg)

### 3
Find the number of distinct business_ids that do not have any reviews. For this query, consider the full review table regardless of matching users. (given as count)

### 4
Amongst users from the user table that have at least 200 reviews, find the 2 “most similar” users, 
where the similarity of two users is defined as
the number of shared businesses they’ve visited. Assume a user has visited a business if he
or she has a review or a tip on a business. (given as user_id1, user_id2, similarity)

For partial credit you may calculate a visit only as review. *I would suggest starting by solving this problem, then extend to have a visit 
as review or tip.*  Again count business_ids even if there is no corresponding id in the business table.

### 4 extra credit 
Amongst users from the user table that have at least 200 reviews, find the 2 “most similar” users, where the similarity of two users is defined as
the fraction of shared businesses they’ve visited. Specifically if A and B are the sets of businesses the two users have
visited, define similarity as |A∩B|/|A∪B| , where |x| is the number of elements in x. Assume a user has visited a business if he
or she has a review or a tip on a business. (given as user_id1, user_id2, similarity)


### 5
Consider the users who have at least 50 reviews, find the users who have the lowest average star reviews (might be one or more).
From this set for each of the reviewers, find the business they gave the highest score to.
(alterntive description: Amongst users from the user table  with more than 50 reviews, for the user id whose reviews have the lowest average star rating, find the names and ids of the businesses that user gave the highest star ratings to.)
*Use an outer join to account for business_id's that do not match records in the business table*

(given as user_id, user_name, business_id, business_name)

