#HW0 by Felipe Alamos

We only need one folder, we can call it Data, with 5 files.
Files:
-Animals.csv. Columns would be: id:int, name:string, age: int, species_id: int, cage_id:int
-Cages.csv. Columns would be: id: int, name:string, building_id:int, lastFeedTime: datetime, zookeeper_id: int
-Zookeepers.csv Columns: id: int, name:string, age:int
-Species.csv Columns: id:int, name:string
-Buildings.csv Columns: id:int, name:string

Operations
- Add animal:
Insert new row in the Animals file

- Move all snakes to a new building:
1. Find snakes id in Species file
2. Filter all the rows of the Animals file where species_id is the id found in (1)
3. Find the first row (cage) in the Cages file where the building_id is the desired one (the one where we want to move the snakes).
4. Change the cage_id of all rows found in (2) to the one found in (3)

- Find the hungriest cage (e.g. the cage that has gone the longest since last feeding)
I would order the Cages file according to lastFeedTime column from highest (most recent) to lowest (oldest), and return the last row of the table.