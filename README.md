# KaggleDatabaseLoader
Python script to load data from csv's into a MySQL database

Needed CSV files are found in these Datasets from kaggle.com:

Iris Species

Human Resources Analytics

Exoplanet Hunting in Deep Space

... more to come in the following months.

There is a file called dbconnection.py, you must enter your information for your database there.
In main, you must enter the file paths to each csv file, I have not changed the names of those files in the functions.
This is due to the fact that each file is made specifically for each csv file.

The hostname, user name, shorthand username, and databse should be setup beforehand, begin by typing:

mysql -u root -p

then setup the database from there.

If you do not have MySQLdb, you can install it with pip:

sudo apt-get install python3-dev libmysqlclient-dev

pip install MySQL-python

If you do not have pip:

apt-get install python3-pip

OR:

wget https://bootstrap.pypa.io/get-pip.py

sudo python3 get-pip.py
