import MySQLdb

#You must specify your localhost, login name, password for MySQL
#as well as the Database you are inserting into.
#This is to be setup beforehand while logged into MySQL.
def dbstuff():
    database = MySQLdb.connect('localhost', 'usrname', 'password', 'Database name')
    return database.cursor(), database
