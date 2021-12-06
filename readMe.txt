1. Import the included "meshJoin" Java file into your Eclipse Project.

2. Import the two JAR files (commons-collections4-4.4 , mysql-connector-java-8.0.26) to your project. These files are included with this submission.
	a) To import the JAR files, right click on your project name under Package Explorer (Top Left), and go to Properties
	b) Click on "Java Build Path" on the navigation window (4th option from the top)
	c) Select "Libraries" from the tabs on the top
	d) Click on "Add External JARs...", and select the relevant JAR files to import

3. Run the Project now.

4. You will be prompted to enter your MySQL details (username, password, and the name of the schema where existing Transactional and Masterdata is in).
   Enter the correct details to move on ahead, else the Project will not run, and give the following error:- "Error while connecting to the database".

5. After running the project, please wait for a bit so that the meshJoin algorithm can run its course.

6. You will be informed when the algorithm has completed. Now you will be able to see the Data Warehouse created in the same schema
   where the Transactional and Masterdata is in.