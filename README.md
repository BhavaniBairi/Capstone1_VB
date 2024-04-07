YouTube Data Harvesting and Warehousing
------------------------------------------------------------------------------------------------------
1. @Title: YouTube Data Harvesting and Warehousing--README
2. @Description: To create a Streamlit application that allows users to access and analyze data from multiple YouTube channels
------------------------------------------------------------------------------------------------------

-----------------------------
Quick Configuration Guide:
-----------------------------
To get the quick access to the different packages of python:
Create a virtual evnironment under the folder in which you are creating python(.py) file in Visual Studio Code and install below packages in cmd prompt:

1. pip install google-api-python-client (to import googleapiclient.discovery) 
2. pip install pymongo (to import pymongo)
3. pip install psycopg2 (to import psycopg2)
4. pip install pandas (to import pandas as pd)
5. pip install streamlit (to import streamlit as st)
6. python -m pip install -U matplotlib (to import matplotlib.pyplot as plt)

--------------------------
Features:
--------------------------
YouTube Data Harvesting and Warehousing supports following features:
1. Ability to input a channel id
2. Ability to retrieve and strore the data to data lake
3. Ability to migrate the stored data to SQL Database
4. Ability to view the tables with data by category. Ex:Channel, playlists etc.,
5. Ability to display the SQL Queried data in Streamlit application
6. Ability for viziualization of the data

---------------------------------
Python code includes:
---------------------------------
1. Functional blocks to get the required details from the youtube
2. Functional block to store the data to datalake
3. Functional blocks to create tables and migrate the data to SQL Database
4. SQL Queries to retrieve the data based on requirement and display data in Streamlit application

