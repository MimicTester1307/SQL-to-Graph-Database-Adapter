# SQL-to-Graph-Database-Adapter

This is the code for our final year CSIS project at Ashesi University, supervised by Dr. Umut Tosun. The goal is to build a tool that facilitates the movement of data between relational (SQL) databases and Graph (Neo4j) databases. This is an open-source project, and can hopefully be improved upon.

This is primarily a backend system, but a client-side integration might be completed eventually. To test the system, please ensure that you are running a Python3 virtual environment. To create one, see [this video](https://realpython.com/lessons/creating-virtual-environment/). If you prefer an article, see [this](https://realpython.com/python-virtual-environments-a-primer/) instead.

After the virtual environment is created, you can clone this repository to your system (in the virtual environment). Other requirements that need to be satisfied to test this project include:

- have your own relational datIn the base directory of the cloned repository, create a `.env` file.then go to `src/config.py`. abase that contains tables that could be extracted. In addition, you must have appropriate permission to the database. Managed user access is suggested. You will need the database credentials later on.
- create a Neo4j AuraDB instance (More info [here](https://neo4j.com/cloud/aura-free/)). You get one instance free forver, so you can use it to test. Current authentication is basic, i.e. using passwords. Save the details too.
- Use the following steps to configure the system for use:

  - in the base directory of the cloned repository, create a `.env` file.
  - open `src/config.py`. There you will find the names of the different environment variables used in the system as they are called using `os.environ.get`. Fill those names into the `.env` file in appropriate formats with your SQL database credentials and your Neo4j AuraDB instance credentials. The only currently supported SQL dialect is MySQL, but that can be easily modified.
  - in the `main.py` file at the base directory of the clone repository, change the `table_names` values to those of tables that you want to extract from your relational database.
  - run  `pip install -r requirements.txt` to install the require dependencies for the system. If installing Apache Airflow poses a problem, run the following command: `pip install "apache-airflow[celery]==2.5.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.0/constraints-3.7.txt"`, then try running the previous command again. All installations should work well at this point.

...And you're good to go!


To run the system, simply:
- run the `main.py` file using your favourite IDE, or from the terminal type `python3 main.py`.
- observe (possibly side-by-side), as the system runs and your node labels are generated.

In the future, we could create a provision for automatically providing the relationship you want to examine between your nodes. Automatically doing so now was more complex than we earlier thought.
