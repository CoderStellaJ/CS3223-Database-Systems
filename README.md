# CS3223-Database-Systems

### Running the program
1. configuration for RandomDB
![RandomDB configuration](./imgs/randomdb.png)
set program arguments to `DBname numrecords`

2. run RandomDB and generate `.md .stat .txt` files

3. configure ConvertTxtToTbl 
![RandomDB configuration](./imgs/convertTxtToTbl.png)
set program arguments to `DBname`

4. run ConvertTxtToTbl and generate `.tbl` file

5. configure QueryMain
![RandomDB configuration](./imgs/queryMain.png)
set program arguments to `queryfilename resultfile pagesize numbuffer`

6. run QueryMain and generate output file

### Adding new join algorithm
1. `PlanCost.java` add cost to your join algorithm
2. Add java file of your algorithm into operators folder. Please refer to `Join.java` and extends this class.
3. Modify `JoinType.java`, add 1 to `numJoinTypes()` 
4. `RandomInitialPlan.java createJoinOp()` generates a random number to select which join algorithm to use. 
You can change `joinMeth` to the type you implemented for debugging

Note: we specified our file path in `RandomDB.java`. Please change it when you pull from github.

### Experiments
1. Define `.det` file
    - First line: number of columns
    - Second line: size of tuple = number of bytes
    - Third line onwards: colname, coltype, range of the values, key type PK/FK/NK, attribute size (numbytes)

2. run `RandomDB` numrecords argument should be larger than 10000.
    - `AIRCRAFTS` 20000
    - `CERTIFIED` 30000
    - `EMPLOYEES` 40000
    - `FLIGHTS` 40000
    - `SCHEDULE` 40000

3. run `ConvertTxtToTbl`

4. Modify code to execute a specific plan
    - change `joinMeth` in `createJoinOp()` of `RandomInitialPlan.java`
    - return initial plan directly `return rip.prepareInitialPlan();` in `getOptimizedPlan()` from `RandomOptimizer.java`
    - change optimizer in `QueryMain.java` to `RandomOptimizer`