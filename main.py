from driver import datamunging, dbdrive


# Driver function initiation
def initiation(val):
    print(val)
    db_insert_values = datamunging()
    return_val = dbdrive(db_insert_values)
    if return_val:
        print('db insertion success')


# Main Initiation
if __name__ == '__main__':
    initiation('Run Initiated')
