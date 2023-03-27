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

source_file_loc = (r"C:\Work\BIA\MediaScape.csv")

df = pd.read_csv(source_file_loc,encoding ='latin1')
df.columns=['DOMAIN', 'CAMPAIGN_NAME', 'B2B_CAMPAIGN_FLAG', 'CHANNEL', 'IMPRESSIONS', 'SPEND', 'PUBLISHER_CNT', 'FIRST_SEEN', 'LAST_SEEN']
df['SPEND'] = pd.to_numeric(df['SPEND'].str.replace('[^\d\.]', ''), errors='coerce')

# insert df into snowflake
# Note: Always use lower case letters as table name. No matter what
print(df)
connection = engine.connect()
df.to_sql('mediascape', engine, if_exists='replace', index=False, index_label=None, chunksize=None,
dtype={'FIRST_SEEN': sqlalchemy.Date, 'LAST_SEEN': sqlalchemy.Date}, method=None) 
connection.close()
engine.dispose()

print('Data successully imported')
