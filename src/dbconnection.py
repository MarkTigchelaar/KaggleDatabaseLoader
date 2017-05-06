import MySQLdb

#You must specify your localhost, login name, password for MySQL
#as well as the Database you are inserting into.
#This is to be setup beforehand while logged into MySQL.

host = 'localhost'
username = 'usrname'
password = 'password'
database = 'Database name'

def dbstuff():
    database = MySQLdb.connect(host, username, password, database)
    return database.cursor(), database
