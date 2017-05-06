from IrisLoader import *
from HRLoader import *
from KeplerLoader import *

def main():
    #add the last / for the directory path.
    path = ''# all CSVs need to be in same folder
    irisLoader(path)
    HRLoader(path)
    #KeplerFluxLoader(path)
    #dropAll()

def dropAll():
    dropIrisTables()
    dropHR()
    dropKeplerTables()
    print("done.")


if __name__ == "__main__":
    main()
