# Homework 1: Relational Algebra

## Introduction
This homework will require you to solve a few problems using relational algebra. Be sure to have understood the material in chapters 2,6-6.1,

## Dataset

This dataset is roughly based on a subset of https://www.kaggle.com/gabrio/board-games-dataset -- some strings have been changed for easy rendering. It captures information about boardgames and their designers.

* the relation _games_ contains basic information about the boardgames and their score from BoardGameGeek (g_id, name, avgscore, numvotes, minplayers, maxplayers)
* the relation _designers_ containers the designers id, name, and home country (des_id, designer, country)
* the relation _desgame_ is the mapping of designers and games (des_id,g_id)


## Submission
You will submit the homework in your git repository via chisubmit for assignment hw1. Please use the site:
[RelaX](http://dbis-uibk.github.io/relax/index.htm) to solve the problems. Go through the relational algebra tutorial on the site!

We are providing you with datasets which should be used to solve the problems. Use the
following gist: `fb6c717670c19007832e0498e431175f`

To set the gist for your HW in relax, click the drop down in the upper left corner, paste the gist ID, and click load.  You should see the schema on the left.

Note that the exact syntax for relax differs from how you would write the operators (especially aggregation see below), but the functionality is the same.

*You have to submit 2 files for each problem:* (1) your query in relational algebra
(the file extension should be .ra - relational algebra) (2) obtained results (the file extension
should be .dat). All of the files you should modify exist already in the answers directory! If you want to double check your answer, copy and paste from your file into relax and see if it 'compiles'.

For example, let’s assume that there is a problem 0, which
asks you to list the id and names of all designers. You have to create the following files:

```
file: 0.ra
π des_id,designer (designers)
file: 0.dat
19	Joseph M. Balkoski
11	Marco Maggi
15	Ken Dunn
20	Ed Beach
27	Craig Besinque
21	Mike Belles
2	Phil Eklund
26	Francis Tresham
13	Corey Konieczka
25	John Rodriguez
23	Joel Toppen
0	Isaac Childres
5	Marc Ripoll
17	Mark Simonitch
24	Flo de Haan
10	Roberto Di Meglio
22	Chris Withers
7	Vlaada Chavtil
8	Rob Daviau
9	Matt Leacock
28	Walter Vejdovsky
3	Jens Drgemller
14	Andreas Seyfarth
1	Philippe Keyaerts
12	Francesco Nepitello
4	Helge Ostertag
6	John H. Butterfield
16	David Sirlin
```


There should be no new line after the value David Sirlin. Do not include the header for the
results (e.g. student.ID student.name). You have to copy the text of your relational algebra
query and statement result to the appropriate files. The order of the attributes in the
projections matters (use the order we describe) Note, that
each value in a line is separated by a single tab character (\t).

### Sample aggregation operations
Aggregation is a bit different, see the following exampels of how to do aggregations

List the mean number of minplayers as avgminply for all games
`γ avg(minplayers) -> avgminply (games)`

Show the mean of avgscores as scrbyminplayer,grouped by the number of minplayers
`γ minplayers; avg(avgscore) -> scrbyminplayer (games)`


List the mean number of minplayers as avgminply and mean maxplayers as avgmaxply for all games
`γ avg(minplayers) -> avgminply, avg(maxplayers) -> avgmaxply (games)``

# Problems
The given is providing you with expected output schema. Please adhere to this schema for full credit.

## 1
List the name of every game that has an avgscore greater than 9 (given as name)

## 2
List the number (count) of designers for each country. Exclude designers that have no country (eg no null) (given as country, cnt)

## 3
List the name and avgscore for all of games by Corey Konieczka, sorted by avgscore descending. Assume you do not know Corey's des_id! (pro tip: wrap string constants with 'single quotes') (given as name, avgscore)

## 4
List the designers whose worst rated game (as minscore) is avgscore is higher than 8.0 (given as designer, minscore)

## 5
Give the name of the designer who has designed the most number of games (given as designer).

## 6
List the number of games that do not have a designer (single attribute given as cnt, single record answer expected)


### Additional practice Problems
If you want additional practice problems, solve the following practice problems from the book and place the answers in the practice directory. We will try to set up some auto-feedback on these:

- 6.10
- 6.11
- 6.13
- 6.14
 
 
We are providing you with datasets which should be used to solve the practice problems. Use the
following gists (problem(s): gist):

-  6.10: 40be6fdf9c2a7acda6a4c1260f210802
-  6.11 and 6.13: 82bfb952e15b0a7364f6caa74c05ea7a
-  6.14: c31aedf00e0f222591751cebe4b55a5d

###  Additional pracitce problem information
Problem 6-10-b: there are only 2 semesters in a year and we assume that the Spring semster
is the first one of the year.

The name First Bank Corporation was replaced with FirstBankCorporation for problems 6-11 and 6-13. 
Similarly, the name Small Bank Corporation was replaced with SmallBankCorporation.

Problem 6-11. We violated the primary key constraint for the company relation 
(the company_name attribute does not serve as the primary key for the table) to 
fulfill the assumption from exercise 6.11.e (companies may be located in several 
cities). However, this violation does not cause any problems for solving: 6-11 
or 6-13.


