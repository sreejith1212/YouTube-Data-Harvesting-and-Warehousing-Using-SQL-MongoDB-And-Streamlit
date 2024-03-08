# YouTube Data Harvesting and Warehousing Using MongoDB, SQL And Streamlit
Building a simple user friendly UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.

## Pre-Requisite
1) Python: Install Python
2) MySQL: Install MySQL database server and client on your system.
3) MongoDB: Install MongoDB on your system.

## Installation
1) Clone the repo, create and activate the environment for the project.
2) Install all required packages from requirements.txt file using command: "pip install -r requirements.txt"

## Usage
1) To start the app, run command: "streamlit run youtube_dataharvest.py"
2) Searching the channel using Channel ID.
3) Retrieve and save the channel data to MongoDB.
4) Migrating the channel data from MongoDB to SQL database.
5) Get general insights about the migrated channel data in the "Q & A" section.

## Features
1) Setting up Streamlit app: Using Streamlit application to create a simple UI where users can enter a YouTube channel ID, view the channel details, save the channel data to MongoDB, select and migrate channel data from MongoDB to the Mysql Database. Also we can display the SQL query results in the Streamlit app to get more insights on the data collected.
2) Connect to the YouTube API: Using YouTube API to retrieve channel, video and comment data. We can use the Google API client library for Python to make requests to the API.
3) Store data in a MongoDB data lake: Retrieved data from the YouTube API is stored in the MongoDB data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily. Using "pymongo"(Python driver for MongoDB) to interact with MongoDB database.
4) Preprocessing Data: Using Pandas to preprocess the data before storing it to the SQL Database. Also to preprocess the SQL database queried results, before displaying it on the streamlit UI.
5) Migrate data to a SQL data warehouse: Collected data for multiple channels can be migrated from MongoDB to a SQL data warehouse. Using MySQL database for this. Using Python SQL library  "pymysql" to interact with the SQL database.
6) Query the SQL data warehouse: Using SQL queries to join the tables in the SQL data warehouse and retrieve data for solving some general questions.
