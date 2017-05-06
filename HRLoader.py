import MySQLdb
from dbconnection import dbstuff

def HRLoader(path):
    dropHR()
    try:
        makeHR()
        f = open(path + "HR_comma_sep.csv", 'r')
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
