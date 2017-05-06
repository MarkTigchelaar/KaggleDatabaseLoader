import MySQLdb
from dbconnection import dbstuff

def irisLoader(path):
    dropIrisTables()
    try:
        makeIrisTables()
        loadIris(open(path + 'Iris.csv'))
        print('Loading complete.')
    except:
        print("Iris table failed to load.")
        dropIrisTables()

"""
    As the input is a csv file, the file must be broken into a List
    in order to be correctly cleaned for insertion into the database
    Input: A file object.
"""
def loadIris(File):
    print('Splitting Iris file into list')
    raw = File.read()
    File.close()
    cursor, database = dbstuff()
    List = raw.split('\n')
    List = List[1:-1]
    for i in range(len(List)):
        print('loading record ' + str(i + 1))
        loader(List[i].split(','), cursor, database)
    cursor.close()
    database.close()


"""
    Inserts each record individually into the MySQL database
    Input: A list, a MySQL cursor object, a MySQL connector object
"""
def loader(record, cursor, database):

    cursor.execute("insert into iris ( Id, SepalLengthCm, SepalWidthCm,  \
                    PetalLengthCm, PetalWidthCm, Species ) values ( " + clean(record) + ")")
    database.commit()


"""
    cleans the iris species of any \n characters, and places quotes around
    each name. This prevents MySQL from complaining about incorrect syntax
    concerning strings.
    Input: A list.
    Returns a string
"""
def clean(record):
    item = ""
    for i in range(len(record)-1):
        item += record[i] + ', '
    last = record[-1]
    last = last.replace(' ', '')
    item += "'" + last + "'"
    return item

"""
    Removes the iris table from the database
"""
def dropIrisTables():
    print('dropping iris table if present')
    cursor, database = dbstuff()
    cursor.execute("drop table if exists iris")
    cursor.close()
    database.close()

"""
    Creates the iris table inside the database
"""
def makeIrisTables():
    print('Making iris table')
    cursor, database = dbstuff()
    TableColumns = "Id int, SepalLengthCm float, SepalWidthCm float, PetalLengthCm float, \
                    PetalWidthCm float, Species varchar(15)"
    cursor.execute("create table if not exists iris (" + TableColumns + ")")
    cursor.close()
    database.close()
