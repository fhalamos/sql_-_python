#Homework 7 [DBs]
The following report introduces the work we have done to accomplish “Homework 7: Data Integration”. 
by Belen Michel Torino and Felipe Alamos

##Assumption
For this homework to run, we assume that the necessary tables from Homework 6 (bike trips) already exist in the db.

##Splitting the work
We generally worked together thorughout the whole assignment, did a lot of pair programming and debugging together. The leadership of each part was as follows:
1.1 Blocking: Felipe
1.2 Computing similarity scores: Belen
2.1 Authoritative restaurant: Felipe
2.2 and 2.3: Together
3. Join Bike Data: Belen

At time we manually filled data with fictional input (such as clusters and matches), so that we both could work even if the previous section was not finished. 


##Design decisions
We created a table called sfinspection_temp that copied all the information contained in table sfinspection, and added columns that we needed for bocking and matching. For example, we created a column called cluster_id which indicated to which block/cluster each row was sent to. We also created a column called ‘matched’, which had a 1 on records that matched with other elements in their clusters, and empty when they did not. We also stored a unique ID for each record, which we called it bussines_id. 
Another column used in the process of matching the restaurants was the bussines_id_match in order to have a unique ID for each group of observations that corresponded to the same restaurant


For the purpose of our blocking, matching, and cleaning we decided to use IDs that uniquely identified each record (weirdly called bussines_id instead of inspection_id) and IDs that uniquely identified the clusters/blocks (called cluster_id). We also created table that linked the cluster ID to the authoritative name and address of each restaurant. 

### Blocking
Record-level blocking: group restaurants that have same postal code.
String blocking: group restaurants whose names start with the same 3 characters
After applying these two methods, all elements withing the same block have the same postal code and 3 characters in the name.

### Similarity Score
For the similarity matching, we created a function that takes 3 columns from the table sfinspection and compares them to the same columns of another record in the table. The 3 columns that we chose were Name, Address, and City. We acknowledge that the City in this specific application is not an interesting column to use for the matching since all of them are San Francisco, but we considered that for an extension of this exercise on a broader database with different cities this would have been an interesting column to use. Acknowledging this we decided to give the city’s similarity score a very low weight in the aggregated similarity score. 
We tried several matching methods and packages. And, for the purpose of practicing, in this assignment we chose to code ourselves the Jaccard method. This is a set method that gives us the ratio between the shared sets (of k-grams)  of both strings and the total sets in both strings. 
As the homework suggested, we used a edit distance-based method. To be more specific, we used the Levenshtein distance from the package jellyfish. This method computes the different inserts, deletes and substitutes that needs to be done in order to go from one string to the compared one. 
Finally, we decided to use the jaro distance algorithm also from the jellyfish package. This was chosen because  it is one of the most popular algorithms for matching since is has a good performance. 
In sum, our function uses these three algorithms to calculate in which extension the compared columns of different observations (restaurants in our case) are the same. Those three comparisons are lately used by the function to give a final weighted average that compares those two observations.  The weights for this final aggregation should be decided with some knowledge of the database and the use of it. We based our decision in simple comparisons of the distribution of the separate results.

### Matching Function
Our matching function uses the similarity score function and calculates the score for every tupple within its block. If it is matched at least with one of the other records it will be marked as matched. And all the observations marked as matched within the same cluster will be considered the same restaurant. 
The threshold for the similarity_score that we used was 0.7 because we found that there were many scores in the extremes (near the 0 or near the 1). An improved way of defining the threshold would involve creating a function that calculates False positive and False negative in the matching (we would have to label some matches manually for testing this).

### Authoritative representative
To decide which would be the name, address, and city to keep in out clean table of restaurants we decided to use the most complete version of the ways in which each of those columns were written i.e. we kept the longest string in each field.
We consider this is a good decision but can still be improved by taking some considerations about the similarity score that the authoritative have with the other strings. We consider that the best option could have implemented would have been a combination that accounts for the completeness of the record and the higher similarity score that it has, in average, with the all the other records.  

### Clean Restaurants
Our table of clean restaurants includes all the matched restaurants as only one observation with the name and address as we obtained them from the authoritative function. We also included the unmatched restaurants with the same name and address they had in the original table. 
The restaurants that have no location (no postal code) were not keept since we considered that an indispensable attribute of the restaurant to clearly identify it in its location/address.	

### Clean Inspections
For the creation of this table, we kept all the information related to the inspection details from the original file. Information relative to the location of the inspection (name and address of business) came from the autoritative restaurant in case of the restaurant being part of a cluster, or we used the same name and address of the inspections table in case that restaurant did not match with anybody. This way, in clean inspections all inspections have name and address, and all inspections whose restaurant matched have the same name and address (the authoritative one).