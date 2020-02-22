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
