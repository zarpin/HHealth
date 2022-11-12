# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from driver import datamunging, dbdrive


def initiation(val):
    print(val)
    db_insert_values = datamunging()
    dbdrive(db_insert_values)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    initiation('Run Initiated')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
