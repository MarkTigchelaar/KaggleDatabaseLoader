import MySQLdb

#       You must specify your localhost, login name, and shorthand name for MySQL
#       as well as the Database you are inserting into.
#       This is to be setup beforehand while logged into MySQL.
def dbstuff():
    database = MySQLdb.connect('localhost', 'your MySQL username', 'short hand username', 'Database name')
    return database.cursor(), database

def main():
    #Do not add the last / for the directory path.
    path = 'Your path to your csv folder'# all CSVs need to be in same folder
    irisLoader(path)
    HRLoader(path)
    KeplerFluxLoader(path)
    #dropAll()

def dropAll():
    dropIrisTables()
    dropHR()
    dropKeplerTables()
    print("done.")

"""
    ### This section loads the iris data into the database ###
"""
def irisLoader(path):
    dropIrisTables()
    try:
        makeIrisTables()
        loadIris(open(path + '/Iris.csv'))
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





"""
    ### This section load the HR data into the database ###
"""
def HRLoader(path):
    dropHR()
    try:
        makeHR()
        f = open(path + "/HR_comma_sep.csv", 'r')
        List = prep(f.read())
        sendIt(List)
        f.close()
        print('Loading complete.')
    except:
        print('HR table failed to load, removing table.')
        dropHR()


"""
    Seperates the concerns of splitting of the records of the csv files
    into lists for easy parsing.
    Removes the first record, as it is the file header.
    Returns a list of lists.
"""
def prep(File):

    List = File.split('\n')
    List.pop(0)
    for i in range(0, len(List)):
        List[i] = List[i].split(",")
    return List


"""
    Drops the HR table from the database
"""
def dropHR():
    print('dropping HRtable if present')
    cursor, database = dbstuff()
    cursor.execute("drop table if exists HRStats")
    cursor.close()
    database.close()


"""
    Creates a new version of the HR table in the database
"""
def makeHR():
    print("Making HR table")
    cursor, database = dbstuff()
    cursor.execute("create table if not exists HRStats (Satisfaction float, LastEval float, \
                    NumOfProjects int, MonthlyHrs int, CompanyTime int, \
                    WorkAccident int, Quit int, Promotion5yrs int, \
                    Dept varchar(12), Salary varchar(7))")
    cursor.close()
    database.close()


"""
    Converts each item in each index to the appropriate type for the table,
    returns the list as an immutable tuple, for the MySQL syntax to work properly.
    Returns a tuple.
"""
def getTuple(L):
    L[0] = float(L[0])
    L[1] = float(L[1])
    L[2] = int(L[2])
    L[3] = int(L[3])
    L[4] = int(L[4])
    L[5] = int(L[5])
    L[6] = int(L[6])
    L[7] = int(L[7])
    L[8] = str(L[8])
    L[9] = str(L[9])
    return tuple(L)


"""
    This function inserts the cleaned items from the HR csv into the database
"""
def sendIt(List):

    print("Loading HR table. Please wait...")
    cursor, database = dbstuff()
    total = str(len(List))
    row = 1
    for L in List:
        tup = getTuple(L)
        cursor.execute("insert into HRStats (Satisfaction , LastEval , \
                        NumOfProjects , MonthlyHrs , CompanyTime , \
                        WorkAccident , Quit , Promotion5yrs , \
                        Dept , Salary) values \
                        ('%f', '%f', '%d', '%d', '%d', \
                        '%d', '%d', '%d', '%s', '%s')" % tup)
        database.commit()
        if row % 100 == 0:
            print('loading row ' + str(row) + '/' + total + " into HRtable")
            print("Loading... please wait...")
        row += 1
    cursor.close()
    database.close()





"""
    ### This section loads up data for the Kepler satillite ###
"""
def KeplerFluxLoader(path):
    dropKeplerTables()
    makeKeplerTables()
    try:
        loadTables(open(path + '/exoTrain.csv', 'r'), 'KeplerTrain')
        loadTables(open(path + '/exoTest.csv', 'r'), 'KeplerTest')
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
        load(current, name, colTable1, cursor, database)
        name = table + '2'
        load(current, name, colTable2, cursor, database)
        name = table + '3'
        load(current, name, colTable3, cursor, database)
        name = table + '4'
        load(current, name, colTable4, cursor, database)
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
    Inputs: A list of lists, table name, string of colNames, cursor object,
    database connector object.
"""
def load(List, table, colTable, cursor, database):
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


if __name__ == "__main__":
    main()
