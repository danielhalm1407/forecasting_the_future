from sqlalchemy import create_engine
from sqlalchemy import inspect, text
import logging
import os
import sys

#Getting the engine

logging.info('Finding current Path')
__file__ = "sql_queries.py"
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0,os.path.join(current_dir, '..'))


# Connect to local database
def get_sql_engine(data_path):
    """
    Returns a SQLAlchemy engine object for connecting to a SQLite database.

    If the engine object has not been created yet, it will be created and stored
    as a static attribute of the `get_sql_engine` function.

    Returns:
        sqlalchemy.engine.Engine: The SQLAlchemy engine object.
    """
    if not hasattr(get_sql_engine, 'engine'):
        get_sql_engine.engine = create_engine('sqlite:///' + data_path)
    return get_sql_engine.engine

# Connect to cloud engine in supabase
def get_supabase_engine(user, password, host, port, database):
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    return engine



# Create a table with Pandas
def make_table(df, name, engine, if_exists='append'):
    with engine.connect() as conn:
        pass
    df.to_sql(name, engine, if_exists=if_exists, index=False)


# Dynamically create SQL Queries

def infer_sql_type(series) -> str:
    """
    Infers the appropriate SQL data type for a pandas Series.

    Args:
        series: pd.Series
            The pandas Series for which to infer the SQL type.

    Returns:
        str: The inferred SQL data type as a string.
    """
    # Check if the series is of integer type
    if pd.api.types.is_integer_dtype(series):
        return "INTEGER"
    # Check if the series is of float type
    elif pd.api.types.is_float_dtype(series):
        return "REAL"
    # Check if the series is of boolean type
    elif pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    # Check if the series is of datetime type
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    else:
        # For object types, infer VARCHAR length or fallback to TEXT
        max_len = series.dropna().astype(str).map(len).max()
        return f"VARCHAR({max_len})" if max_len else "TEXT"

def df_to_create_table_sql(df: pd.DataFrame, table_name: str) -> str:
    """
    Generates a SQL CREATE TABLE statement based on the columns and types of a pandas DataFrame.

    Args:
        df: pd.DataFrame
            The DataFrame to analyze.
        table_name: str
            The name of the SQL table to create.

    Returns:
        str: The SQL CREATE TABLE statement as a string.
    """
    cols = []
    # Iterate through each column to infer its SQL type and build column definitions
    for col in df.columns:
        sql_type = infer_sql_type(df[col])
        cols.append(f"{col} {sql_type}")
    # Join all column definitions into a single string
    col_definitions = ",\n    ".join(cols)
    # Format the final CREATE TABLE SQL statement
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {col_definitions}\n);"



# Static SQL QUERIES
# create a table if it doesn't exist
CREATE_TS_TABLE_SQL_QUERY = """
CREATE TABLE IF NOT EXISTS ts_data (
    yq VARCHAR PRIMARY KEY,
    level REAL
);
"""

# drop the table if needed
DROP_TS_TABLE_SQL_QUERY = "DROP TABLE IF EXISTS ts_data;"

# get the standalone rightmove data
GET_RIGHTMOVE_DATA_SQL_QUERY = """
SELECT  
    *
FROM rightmove_data
"""

# get the properties data with additional data like travel times etc
GET_TS_DATA_SQL_QUERY = """
SELECT  
    *
FROM ts_data
"""
## Update existing table with new data

### Update travel time and distance
UPDATE_DIST_AND_TRAVEL_TIME = """
UPDATE properties_data
SET
    travel_time = (
        SELECT temp.travel_time FROM temp_updates temp
        WHERE temp.id = properties_data.id
    ),
    distance = (
        SELECT temp.distance FROM temp_updates temp
        WHERE temp.id = properties_data.id
    )
WHERE id IN (SELECT id FROM temp_updates)
  AND (travel_time IS NULL OR distance IS NULL);
"""

# Update the predicted price/rent per month
UPDATE_PREDICTED_PRICE = """
UPDATE properties_data
SET
    predicted_price_per_bed = (
        SELECT temp.predicted_price_per_bed FROM temp_updates temp
        WHERE temp.id = properties_data.id
    )
WHERE id IN (SELECT id FROM temp_updates)
  AND predicted_price_per_bed IS NULL;
"""

# Insert data with new rows