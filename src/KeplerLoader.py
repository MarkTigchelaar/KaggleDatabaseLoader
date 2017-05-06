import MySQLdb
from dbconnection import dbstuff


def KeplerFluxLoader(path):
    dropKeplerTables()
    makeKeplerTables()
    try:
        loadTables(open(path + 'exoTrain.csv', 'r'), 'KeplerTrain')
        loadTables(open(path + 'exoTest.csv', 'r'), 'KeplerTest')
        print('Loading complete.')
    except:
        print('Tables failed to load, dropping tables')
        dropKeplerTables()


"""
    This function creates 4 seperate tables for the Kepler training data,
    and 4 seperate tables for the Kepler testing data.
    Each csv file has too many columns for MySQL to handle, so each list of
    columns is generated automatically, and then the parsed data is inserted into
    each table.
    Each table has a index on the ID column. This is generated automatically,
    and is not part of the original csv file.
    This is done out of the need to rapidly join tables for various SQL queries.
"""
def loadTables(File, table):
    List = Keplerprep(File.read())
    File.close()
    Listlist = ListSplitter(List)
    length = len(Listlist)
    row = 1
    total = str(length)

    print("Kepler tables are 3197 columns across, and 570, or 5087 rows deep.")
    print("Get a drink, this may take a while...")
    cursor, database = dbstuff()

    #These are repeatedly used, call functions once for each, then save string
    #for re-use.
    colTable1 = colNames(1, 800, False)
    colTable2 = colNames(801, 1600, False)
    colTable3 = colNames(1601, 2400, False)
    colTable4 = colNames(2401, 3197, False)
    while length > 0:
        current = Listlist.pop(length - 1)
        name = table + '1'
        load(current, name, colTable1, 0, 800, cursor, database)
        name = table + '2'
        load(current, name, colTable2, 800, 1600, cursor, database)
        name = table + '3'
        load(current, name, colTable3, 1600, 2400, cursor, database)
        name = table + '4'
        load(current, name, colTable4, 2400, 3197, cursor, database)
        length -= 1
        if row % 100 == 0:
            print('loading row ' + str(row) + '/' + total + " into " + table + " tables")
            print("Loading... please wait...")
        row += 1
    cursor.close()
    database.close()


"""
    This function does the actual insertion into the database.
    It calls helper functions to fill in the extremely long argument list, as well
    as the large numbers of actual data items for each individual record.
    This function will be updated later on to use the MySQLdb function insert many
    for speed increases.
    Inputs: A list of lists, table name, start column, stop columns, string of colNames, cursor object,
    database connector object.
"""
def load(List, table, colTable, start, stop, cursor, database):
    cursor.execute("insert into " + table + " ( " + colTable + ") values (" + items(List, start, stop) + " )")
    database.commit()


"""
    This function converts the list of column items into a string for insertion
    into the table.
    Inputs: A list, start column, end column.
    Returns a string.
"""
def items(List, start, stop):
    items = ""
    for i in range(start, stop):
        items += str(List[i]) + ", "
    items = items[:-2]
    return items


"""
    Splits the Kepler csv into a list, removes the header from the list.
    Input: a string representing the csv.
    Returns a list.
"""
def Keplerprep(Filestring):
    print('Splitting Kepler File into list')
    List = Filestring.split(',')
    print('Removing header')
    return List[3198:]


"""
    Takes the list of rows, splits the rows into cleaned items in a list, appends
    the new list to the original list.
    It does not matter that the first row is now the last. This is because the
    individual records are not given in any particular order in the csv, so there
    is no order to maintain.
    Also, this is before the sublist is split into 4, so everything lines up correctly.
    Input: A list of all items in each column / row
    Returns a list of lists, representing each row as a sub list.
"""
def ListSplitter(List):
    print("Splitting Kepler list into sublists")
    Listlist = list()
    length = len(List)
    while length > 0:
        sublist = list()
        for i in range(3197, 0, -1):
            item = List.pop(length-1)
            if '\n' in item:
                part = item.split('\n')
                item = part[0]
            sublist.append(item)#inserting at 0 is more expensive than reversing.
            length -= 1
        sublist.reverse()# sublist currently backwards.
        Listlist.append(sublist)
    return Listlist


"""
    Creates a string representing the column names in each of the tablesfor the Kepler dataset.
    Is used for the creationg of the table, as well as inserting items into each
    row.
"""
def colNames(start, stop, creating):
    if creating:
        columns = "ID int not null auto_increment, "
    else: columns = ""
    for i in range(start, stop+1):
        columns = columns + "Flux" + str(i)
        if creating:
            columns += " float, "
        else:
            columns += ", "
    columns = columns[:-2]
    if creating:
        columns = columns + ", primary key (ID)"
    return columns


"""
    This function creates a new table inside the database.
"""
def makeKeplerTables():
    print('Creating kepler tables')
    cursor, database = dbstuff()
    TableColumns = colNames(1, 800, True)
    cursor.execute("create table if not exists KeplerTest1 (" + TableColumns + ")")
    cursor.execute("create table if not exists KeplerTrain1 (" + TableColumns + ")")

    TableColumns = colNames(801, 1600, True)
    cursor.execute("create table if not exists KeplerTest2 (" + TableColumns + ")")
    cursor.execute("create table if not exists KeplerTrain2 (" + TableColumns + ")")

    TableColumns = colNames(1601, 2400, True)
    cursor.execute("create table if not exists KeplerTest3 (" + TableColumns + ")")
    cursor.execute("create table if not exists KeplerTrain3 (" + TableColumns + ")")

    TableColumns = colNames(2401, 3197, True)
    cursor.execute("create table if not exists KeplerTest4 (" + TableColumns + ")")
    cursor.execute("create table if not exists KeplerTrain4 (" + TableColumns + ")")

    try:
        print("Adding indicies on primary key")
        cursor.execute("alter table KeplerTest1 add unique index1 (ID)")
        cursor.execute("alter table KeplerTrain1 add unique index2 (ID)")
        cursor.execute("alter table KeplerTest2 add unique index3 (ID)")
        cursor.execute("alter table KeplerTrain2 add unique index4 (ID)")
        cursor.execute("alter table KeplerTest3 add unique index5 (ID)")
        cursor.execute("alter table KeplerTrain3 add unique index6 (ID)")
        cursor.execute("alter table KeplerTest4 add unique index7 (ID)")
        cursor.execute("alter table KeplerTrain4 add unique index8 (ID)")
    except:
        print("Index exists, or tables do not exist")
    cursor.close()
    database.close()


"""
    This function removes the Kepler tables from the database.
"""
def dropKeplerTables():
    print("Removing previous kepler tables if present")
    cursor, database = dbstuff()
    cursor.execute("drop table if exists KeplerTest1")
    cursor.execute("drop table if exists KeplerTrain1")
    cursor.execute("drop table if exists KeplerTest2")
    cursor.execute("drop table if exists KeplerTrain2")
    cursor.execute("drop table if exists KeplerTest3")
    cursor.execute("drop table if exists KeplerTrain3")
    cursor.execute("drop table if exists KeplerTest4")
    cursor.execute("drop table if exists KeplerTrain4")
    cursor.close()
    database.close()
