import os
import dbconfig as cfg
import shutil
from sqlalchemy import create_engine
import pandas as pd
from glob import glob

# User, pw, and db are being imported from dbconfig.py file to mask credentials
db_data = ("mysql+mysqlconnector://{user}:{pw}@localhost/{db}"
           .format(user=cfg.mysql["user"],
                   pw=cfg.mysql["passwd"],
                   db=cfg.mysql["db"]))

# Using 'create_engine' from sqlalchemy to make the db connection
engine = create_engine(db_data).connect()

# set the variables for the filepaths of where the source files are and where to move them to
source = '/your source file path'
dest = '/your destination file path'

# use the glob module to create a list of files in the 'source' path
import_files = sorted(glob(source + '*.csv'))

# get count of files in the 'import_files' list
filecount = len(import_files)

''' create dataframe from files in the source directory '''
# if there are no csv files in the 'source' directory, print message to the console
if filecount < 1:
    print("No files found in the filepath: " + source)

# if there are files in 'source' create the dataframe
else:
    df = pd.concat((pd.read_csv(file).assign(filename=file)
                    for file in import_files), ignore_index=True)

    # move the files from the 'source' path to the 'dest' path
    for file in import_files:
        shutil.move(os.path.join(source, file), dest)

    # print dataframe to the console
    print(df)

    # insert the dataframe into the db table
    df.to_sql(name='prospects', con=engine, if_exists='append', index=False)

    # print success message to the console
    print("file import complete!")



