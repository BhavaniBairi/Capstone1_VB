# Capstone1_VB
YouTube Data Harvesting and Warehousing
------------------------------------------------------------------------------------------------------
@Title: YouTube Data Harvesting and Warehousing--README
@Description: To create a Streamlit application that allows users to access and analyze data from multiple YouTube channels
------------------------------------------------------------------------------------------------------

-----------------------------
Quick Configuration Guide:
-----------------------------
To get the quick access to the different packages of python:
Create a virtual evnironment under the folder in which you are creating python(.py) file in Visual Studio Code and install below packages in cmd prompt:

pip install google-api-python-client (to import googleapiclient.discovery) 
pip install pymongo (to import pymongo)
pip install psycopg2 (to import psycopg2)
pip install pandas (to import pandas as pd)
pip install streamlit (to import streamlit as st)
python -m pip install -U matplotlib (to import matplotlib.pyplot as plt)

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
Functional blocks to get the required details from the youtube
Functional block to store the data to datalake
Functional blocks to create tables and migrate the data to SQL Database
SQL Queries to retrieve the data based on requirement and display data in Streamlit application

