import sqlalchemy
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import pandas as pd

# ESTABLISHING CONNECTION WITH THE SNOWFLAKE WAREHOUSE
 

engine = create_engine(URL(
    account='nua76068.us-east-1',
    user='BURHAND',
    password='Core@123',
    database='BIA',
    schema='PUBLIC',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN'
))

# read csv data from source file

source_file_loc = (r"C:\Work\BIA\Keyword.csv")

df = pd.read_csv(source_file_loc,encoding ='latin1')

df.columns=['KEYWORDID', 'KEYWORD', 'PROJECTID']

# insert df into snowflake
# Note: Always use lower case letters as table name. No matter what
print(df)
connection = engine.connect()
df.to_sql('keyword', engine, if_exists='replace', index=False, index_label=None, chunksize=None, dtype=None, method=None) 
connection.close()
engine.dispose()

print('Data successully imported')
