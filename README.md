# KaggleDatabaseLoader
Python script to load data from csv's into a MySQL database

Needed CSV files are found in these Datasets from kaggle.com:

Iris Species

Human Resources Analytics

Exoplanet Hunting in Deep Space

... more to come in the following months.

At the top of the file, there is a function called dbstuff, you must enter your information for your database there.
In main, you must enter the file paths to each csv file, I have not changed the names of those files in the functions.

The hostname, user name, shorthand username, and databse should be setup beforehand, begin by typing:

mysql -u root -p

then setup the database from there.

If you do not have MySQLdb, you can install it with pip:

pip install MySQL-python

If you do not have pip:

apt-get install python-dev libmysqlclient-dev

apt-get install python3-pip

OR:

wget https://bootstrap.pypa.io/get-pip.py

sudo python3 get-pip.py 

pip install MySQL-python
