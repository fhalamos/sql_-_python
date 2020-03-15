# Homework 2: SQL

Aaron Elmore

30235

## Introduction

For this assignment you will be writing SQL and will be using a PostgreSQL server, which provides a standards-compliant SQL implementation.  
We are using version 10.6 of Postgres, so the documentation at https://www.postgresql.org/docs/10.6/static/ may be useful as well.

You will need access to a psql client that supports version 10 or later. On debian/ubuntu machines this is as simple as running
`sudo apt install postgresql-client-common postgresql-client-10`

You will connect to Postgres to use a preloaded database using:

`psql -d boardgames -U dbstudent -h addison.cs.uchicago.edu`

when it asks you for the password use *dbrocks33*  
Note that you can add a password to avoid having to type in the password everytime
https://www.postgresql.org/docs/10.6/static/libpq-pgpass.html


## Dataset
We will use a slightly expanded database of the boardgames dataset.
This now includes attributes minplaytime and maxplaytime for the games relation.
In addition there is a categories relation and gamecat relation that maps games to categories.

Once connected, there are two kinds of commands useful to a database user. The first kind are the psql client meta-commands
and the second are SQL statements (which end in a ;).
The most important command is \?, which gives you help on meta-commands. There are two others you will find very useful.  

First, you can list the tables in the database with \dt

```
boardgames=> \dt
           List of relations
 Schema |    Name    | Type  |  Owner   
--------+------------+-------+----------
 public | categories | table | postgres
 public | desgame    | table | postgres
 public | designers  | table | postgres
 public | gamecat    | table | postgres
 public | games      | table | postgres
(5 rows)


```

Second, you can view the schema  of a given table with \d *table_name*

```
boardgames=> \d games
                         Table "public.games"
   Column    |          Type          | Collation | Nullable | Default
-------------+------------------------+-----------+----------+---------
 g_id        | integer                |           | not null |
 name        | character varying(100) |           | not null |
 avgscore    | numeric(7,6)           |           |          |
 numvotes    | numeric(6,1)           |           |          |
 minplayers  | smallint               |           |          |
 maxplayers  | smallint               |           |          |
 minplaytime | smallint               |           |          |
 maxplaytime | smallint               |           |          |
Indexes:
    "games_pkey" PRIMARY KEY, btree (g_id)
Referenced by:
    TABLE "desgame" CONSTRAINT "desgame_g_id_fkey" FOREIGN KEY (g_id) REFERENCES games(g_id)
    TABLE "gamecat" CONSTRAINT "gamecat_g_id_fkey" FOREIGN KEY (g_id) REFERENCES games(g_id)
```

The second class of useful commands are SQL commands. All SQL queries in PostgreSQL must be terminated with a semi-colon.
For example, to get a list of 10 records in the `games` table, you would type:

`SELECT * FROM games LIMIT 10;`

This query  requests a max 10 rows from the table. Using **LIMIT**  in this manner one can explore the data small bits at a time. If you really wanted to produce all the records, though, the query is:

`SELECT * FROM games;`

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

In years past one student gave the following advice for connecting from OS X. I cannot vouch for its accuracy or safety -- *so use at your own risk*!



https://eggerapps.at/postico/


Here is how you'd connect to this online database.



Given this from the assignment...



`psql -d boardgames -U dbstudent -h addison.cs.uchicago.edu`

You'd connect by opening Postico and selecting "New Favorite," entering in

```
host: addison.cs.uchicago.edu
user: dbstudent
password: dbrocks33
database: boardgames
```



Depending on how you connected, you might get a warning that it can't verify this host. I ignore that in this case or use SSL :)

After connecting, you can click on the tables and examine them quickly. To run a query, just hit "SQL Query" in the left sidebar and write some valid sql. Your queries will be saved, so you can click the back button below the text box.


Note: All queries must end in ";" for the assignment, but Postico might allow you to execute the query without it. Just be sure to double check.
echnically, the `page_id` is enough as well but we prefer the page name.


### Submission
You will submit the homework in your git repository via chisubmit for assignment hw2.

You have to submit 1 file for each item in a problem:  your query in SQL
(the file extension should be .sql).

For example, let’s assume that there is a problem 0 and item a, which
asks you to find the distinct names of all departments. You have to create the following file named 0a.sql:

Contents:

`select distinct name from games limit 10;`

All the .sql  files for your answers are already creates, so you just need to insert your answers.
*No output is needed*


## Problems
Schemas for output as provided

### 1
 Find games whose name contains the word *edition* with either uppercase or lowercase 'e'. (given as name)

### 2

Find the 10 longest game titles (by characters) in that allow for between 3 and 5 players inclusive (e.g. games that need at minimum 3 people to play and support a maximum of 5 players) . (given as name, namelen)


### 3
Find the category id and count of games in the category, for categories that have at least 15 games in them. (given as c_id, cnt)

### 4
Find the category name of the most 5 popular categories, where popularity is defined as the highest average/mean of avgscore for games in the category (given as category, avg)


### 5
Extend the query for problem 4 to only show categories that have at least 15 games in them. (given as category, avg)



### 6
For all groupings of minplayers and maxplayers (e.g.  group by minplayers,maxplayers) show the minimum number of hours and maximum number of hours sorted minplayers and then maxplayers (ascending for both). Note that min/maxplaytime is given in minutes. Do not round or truncate hours. (given as minhours,maxhours,minplayers,maxplayers)


### 7
For each category show the designer that has the longest possible game as max (maxplaytime) order by category descending -- considering only the games that have designers associated with them (e.g. you can omit games that have no designer).
Subsequently, only consider categories that have at least one designer associated with its respective games (e.g. if no designer exists for any game in a category, then omit the category.)
In case of a tie for the longest game and/or if a game has multiple designers, show all designers. Do not show the same designer multiple times per category. 
*Use at least one common table expression to get this result for full credit.* (final output relation given as designer,category,max)
