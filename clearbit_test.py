from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import pandas as pd
import clearbit
import json

engine = create_engine(URL(
    account='nua76068.us-east-1',
    user='BURHAND',
    password='Core@123',
    database='BIA',
    schema='PUBLIC',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN'
))

# Set up the Clearbit API key
clearbit.key = 'sk_de55582d62c786acc695b1711a9be772'



# Define the SQL query to retrieve domain names from the Snowflake table
query = 'SELECT DOMAIN FROM "BIA"."PUBLIC"."COMPANY_DOMAIN"'

# Execute the query and fetch the results as a Pandas dataframe
domains_df = pd.read_sql_query(query, engine)


# Create an empty dataframe to hold the results
results_df = pd.DataFrame()

# Iterate over the rows of the domain dataframe and make API calls to Clearbit
for index, row in domains_df.iterrows():
    domain = row['domain']
    try:
        company = clearbit.Company.find(domain=domain, stream=True)
        # If this is the first company, rename the columns to include all keys
        if index == 0:
            results_df = pd.DataFrame(columns=list(company.keys()))
        results_df = results_df.append(company, ignore_index=True)
    except clearbit.errors.ClearbitError:
        print(f"Error retrieving data for domain: {domain}")
        

# converting the dataframe dictionaries into columns
category_df = results_df['category'].apply(pd.Series)
metrics_df = results_df['metrics'].apply(pd.Series)
geo_df = results_df['geo'].apply(pd.Series)
twitter_df = results_df['twitter'].apply(pd.Series)
twitter_df = twitter_df.rename(columns={'id': 'Twitter_Id'})
twitter_df = twitter_df.rename(columns={'location': 'Twitter_location'})

df = pd.concat([results_df.drop(['category','metrics','geo','twitter'], axis=1),category_df, metrics_df,geo_df,twitter_df], axis=1)


# Convert the list column to a JSON string
df['tech'] = df['tech'].apply(lambda x: json.dumps(x))
df['techCategories'] = df['techCategories'].apply(lambda x: json.dumps(x))
df['tags'] = df['tags'].apply(lambda x: json.dumps(x))


# Insert the company data into a new Snowflake table
connection = engine.connect()
columns_to_drop = ['following','identifiers','linkedin', 'domainAliases','site','facebook', 'crunchbase', 'parent', 'ultimateParent']
existing_columns = [col for col in df.columns if col not in columns_to_drop]
df = df[existing_columns]

df.to_csv(r"C:\Work\test_1.csv", encoding='utf-8', index=False, sep=',')
print(df)

df.columns = df.columns.str.upper()
df.to_sql('clearbit', engine, if_exists='replace', index=False, index_label=None, chunksize=None, dtype=None, method=None) 

print('Data successfully imported')

# Close the Snowflake connection
connection.close()
engine.dispose()
