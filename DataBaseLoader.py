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
    This section loads the iris data into the database
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


def loader(record, cursor, database):

    cursor.execute("insert into iris ( Id, SepalLengthCm, SepalWidthCm,  \
                    PetalLengthCm, PetalWidthCm, Species ) values ( " + clean(record) + ")")
    database.commit()


def clean(record):
    item = ""
    for i in range(len(record)-1):
        item += record[i] + ', '
    last = record[-1]
    last = last.replace(' ', '')
    item += "'" + last + "'"
    return item


def dropIrisTables():
    print('dropping iris table if present')
    cursor, database = dbstuff()
    cursor.execute("drop table if exists iris")
    cursor.close()
    database.close()


def makeIrisTables():
    print('Making iris table')
    cursor, database = dbstuff()
    TableColumns = "Id int, SepalLengthCm float, SepalWidthCm float, PetalLengthCm float, \
                    PetalWidthCm float, Species varchar(15)"
    cursor.execute("create table if not exists iris (" + TableColumns + ")")
    cursor.close()
    database.close()





"""
    This section load the HR data into the database
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


def prep(File):

    List = File.split('\n')
    List.pop(0)
    for i in range(0, len(List)):
        List[i] = List[i].split(",")
    return List


def dropHR():
    print('dropping HRtable if present')
    cursor, database = dbstuff()
    cursor.execute("drop table if exists HRStats")
    cursor.close()
    database.close()


def makeHR():
    print("Making HR table")
    cursor, database = dbstuff()
    cursor.execute("create table if not exists HRStats (Satisfaction float, LastEval float, \
                    NumOfProjects int, MonthlyHrs int, CompanyTime int, \
                    WorkAccident int, Quit int, Promotion5yrs int, \
                    Dept varchar(12), Salary varchar(7))")
    cursor.close()
    database.close()


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
    This section loads up data for the Kepler satillite
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
    while length > 0:
        current = Listlist.pop(length - 1)
        name = table + '1'
        load(current, name, 0, 800, cursor, database)
        name = table + '2'
        load(current, name, 800, 1600, cursor, database)
        name = table + '3'
        load(current, name, 1600, 2400, cursor, database)
        name = table + '4'
        load(current, name, 2400, 3197, cursor, database)
        length -= 1
        if row % 100 == 0:
            print('loading row ' + str(row) + '/' + total + " into " + table + " tables")
            print("Loading... please wait...")
        row += 1
    cursor.close()
    database.close()


def load(List, table, start, stop, cursor, database):
    cursor.execute("insert into " + table + " ( " + colNames(start+1, stop, False) + ") values (" + items(List, start, stop) + " )")
    database.commit()


def items(List, start, stop):
    items = ""
    for i in range(start, stop):
        items += str(List[i]) + ", "
    items = items[:-2]
    return items


def Keplerprep(File):
    print('Splitting Kepler File into list')
    List = File.split(',')
    print('Removing header')
    return List[3198:]


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
            sublist.append(item)
            length -= 1
        Listlist.append(sublist)
    return Listlist


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
