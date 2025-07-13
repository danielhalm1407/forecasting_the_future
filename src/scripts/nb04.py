

# Response output
import json
import pandas as pd

# File and System Operations
import os
import sys

# Data Visualisation
import seaborn as sns
import matplotlib.pyplot as plt

# Regressions
from sklearn.linear_model import LinearRegression

# Saving out data
from sqlalchemy import inspect, text

# Tracking
import logging


# Set up logging to debug/ keep track

logging.basicConfig(format='%(asctime)s [%(levelname)s] [%(filename)s] %(message)s', level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)


# DIRECTORY SETUP

### Find the directory of the current file
__file__ = "nb04.py"

logging.info('Finding current Path')
current_dir = os.path.dirname(os.path.abspath(__file__))

## Set the parent to be the current path of the system
# # (so one can import the custom package)
logging.info('Importing Custom Package...')
sys.path.insert(0,os.path.join(current_dir, '..'))
import rental_utils
# Import the functions sub-package
from rental_utils import functions as rent
# Note that whenever rent.function_name is called, 
# all the required packages are imported in the background anyway

# Import the sql queries sub-package
from rental_utils import sql_queries as sqlq

logging.info('Imported Custom Package')


## Set Up The Paths of the Key Outside Directories/Files
logging.info('Setting up other paths...')
credentials_file_path = os.path.join(current_dir, '..', '..', "supabase_credentials.json")
data_folder_path = os.path.join(current_dir, '..', '..', "data")

# open the  credentials file and load the data into a variable
with open(credentials_file_path, "r") as f:
    credentials = json.load(f)


    


# PRIMARY RUNNING


## Load in the complete data from the corresponding table in the database
logging.info('Getting Data from the Database')
engine = sqlq.get_sql_engine(f"{data_folder_path}/properties.db")
with engine.connect() as connection:
    properties_data = pd.read_sql(text(sqlq.GET_PROPERTIES_DATA_SQL_QUERY), connection)
logging.info(f'Data found, with {len(properties_data["id"])} properties')


## Clean the data
reg_data = rent.clean_for_reg(properties_data)

## Make A Scatter Plot of Rent Per Bed Against Travel Time
# allow the user to choose
show_plot = input("Do you want to display the plot? (y/n): ").strip().lower()

if show_plot == 'y':
    sns.regplot(x='travel_time', y='price_per_bed', data=reg_data)
    plt.xlabel('Travel Time')
    plt.ylabel('Price per Bed')
    plt.title('Price per Bed vs Travel Time with Line of Best Fit')
    plt.show()
else:
    print("Plot display skipped.")



# MAKE PREDICTIONS OF RENT PER BED BASED ON THE RENTAL DATA
logging.info('Making Predictions')
## Select features and target
X = reg_data[['travel_time', 'bathrooms']]
y = reg_data['price_per_bed']

## Create and fit model
model = LinearRegression()
model.fit(X, y)

## To generate predictions 
predictions = model.predict(X)

## Add predictions to dataframe
reg_data.loc[:,'predicted_price_per_bed'] = predictions

logging.info('Saving Out Predictions')
# Save Out The Data With Predictions
##### create a temporary table
sqlq.make_table(reg_data[["id", "predicted_price_per_bed"]], "temp_updates", engine, if_exists="replace")

##### run a single SQL statement to update properties_data using temp_updates (fill in missing data)
with engine.begin() as connection:
    connection.execute(text(sqlq.UPDATE_PREDICTED_PRICE))


logging.info('Predictions Saved Out to Local Database')


# Save out to a table in the supbase database
## Connect to the engine
supabase_engine = sqlq.get_supabase_engine(
    user="postgres",
    password=credentials['password'],
    host=credentials['host'],
    port=5432,
    database="postgres"
)

## Execute the CREATE TABLE query to create a blank table if it doesn't already exist
with supabase_engine.begin() as connection:
    connection.execute(text(sqlq.CREATE_TABLE_SQL_QUERY))

## find the ids that already exist
with supabase_engine.connect() as conn:
    existing_ids = conn.execute(text("SELECT id FROM properties_data")).fetchall()
## filter out only the ids that will be unique to the existing table
existing_ids = {row[0] for row in existing_ids}
new_rows = reg_data[~reg_data['id'].isin(existing_ids)]


# fill in the data into the table
sqlq.make_table(new_rows, "properties_data", supabase_engine)


logging.info('Predictions Saved Out to Cloud Database')