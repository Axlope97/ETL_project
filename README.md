# ETL and visualization project 
This projects goal is to develop an **ETL pipeline**, orchestrated with **Apache Airflow**, to transfer data from a **Postgres Database** to a **MySQL Database** and then connect it with **Power Bi** and create a visualization. 
1. **Connection and Extraction**: The first script is the one called **Extraction**. I've created the two functions to stablish the connection with the source database and the destiny, in order to call them through all the project. Both of them takes the credentials needed to stablished a connection from a **JSON** file, containing the database name, user, password, host and port. The last function takes all rows from a determined 
2. **Transformation**: After a quick view, some of the fields in the source database where corrupt. There was several issues related to the format:
- Some of the fields had all the 'a' letters changed over 4's.
- The 'continent' column of the 'countries' table had '/' sings followed with a space. Also, all the 'i' letters were fullfilled with '*' sings.
- The country column of the 'population' table contained '=' sing.
- Finally, in the 'life_expectancy' table, there was null columns.
Once corrected the errors where corrected, the function return the clean DataFrames. 
3.  **Loading**: Firstly, import the **Transformation** function to obtain the clean DataFrames. The function takes the credentials from the **Extraction** script and goes through the DataFrame list, taking the column name of every table and the datatype. Then it checks if the table allready exist in the destiny DB, create the table using the mysql cursor and finally inserting the values.
4. **ETL_project_airflow**: The script orchestrate the scripts to execute them every day and retry every 5 minutes if fail.
![DAG_image](https://github.com/Axlope97/ETL_project/assets/148786116/c37010b5-8b92-4762-8c47-9af82df98168)
After debugging and checking some absent dependencies, the pipeline succeed.
![DAG_succeed](https://github.com/Axlope97/ETL_project/assets/148786116/186d12ac-8965-4cd0-8ffe-0e1202ed4f02)

5. **Visualization**: In this finall step, I used **PowerBI** to craft an interactive report to show all the tables data combined.
![POWERBI_holeview](https://github.com/Axlope97/ETL_project/assets/148786116/7332c252-4688-4c47-a0c5-75795db19de1)

It allows to explore through the continents, range of infant mortality, range of life expectancy, population by millions and countries.
![POWERBI_continentview](https://github.com/Axlope97/ETL_project/assets/148786116/c5753780-1bc2-43de-bdd0-2b4a4a7e5b7e)


