import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import numpy as np
from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import json
from datetime import datetime
import json

def get_db_engine():
    """Returns an Engine element that is connected to the database"""
    load_dotenv()
    dbname = os.getenv("NEON_DB_NAME")
    port = os.getenv("NEON_DB_PORT")
    db_selected = os.getenv("NEON_DB_NAME")
    host = os.getenv("NEON_DB_HOST")
    user = os.getenv("NEON_DB_USER")
    password = os.getenv("NEON_DB_PASSWORD")
    cloud_engine = create_engine(
        f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
    return cloud_engine

def upsert_data(df, table_name, engine, schema, batch_size=10000):
    """
    Upserts a pandas DataFrame to a SQL table, automatically detecting and using
    single or composite primary keys for the conflict resolution.
    """
    # --- Start of Edits ---

    # 1. Dynamically get the primary key columns from the table metadata
    primary_key_columns = [key.name for key in table.primary_key.columns]

    if not primary_key_columns:
        raise ValueError(f"Table '{schema}.{table_name}' has no primary key. Upsert requires a primary key.")

    print(f"Detected Primary Key Columns: {primary_key_columns}")
    

    # Replace NaN with None for SQL compatibility
    df = df.replace({np.NaN: None})

    # Prepare data for insertion
    data = df.to_dict('records')

    total_rows = 0
    with engine.begin() as connection:
        for start in range(0, len(data), batch_size):
            end = start + batch_size
            batch = data[start:end]

            stmt = insert(table).values(batch)

            # --- Start of Edits ---

            # 2. Use the detected primary key columns for the ON CONFLICT clause
            # This works for both single and composite (multi-column) keys.
            do_update_stmt = stmt.on_conflict_do_update(
                index_elements=primary_key_columns,
                set_={
                    # Update columns that are not part of the primary key
                    col.name: stmt.excluded[col.name]
                    for col in table.columns 
                    if col.name not in primary_key_columns and col.name not in skip_repeated_columns
                }
            )
            
            # --- End of Edits ---
            
            result = connection.execute(do_update_stmt)
            total_rows += result.rowcount
            print(f"Batch from {start} to {end} upserted, rows inserted/updated = {result.rowcount}")
    
    end_time = datetime.now()
    total_time = (end_time - start_time).total_seconds()
    
    print(f"Total rows inserted/updated = {total_rows}")

def get_data_query_filtered(engine, schema: str, table_name: str, 
                            columns_to_get: list = [], filters: dict = {}, 
                            geq_dict_filter: dict = {}, leq_dict_filter: dict = {}):
    # Check if columns_to_get is empty to select all columns
    if len(columns_to_get) == 0:
        columns_str = "*"
    else:
        columns_str = ', '.join(columns_to_get)
    
    # Construct the WHERE clause based on the filters dictionary
    where_clauses = []
    for column, values in filters.items():
        if not isinstance(values, list):
            values = [values]
        values_str = ', '.join([f"'{value}'" for value in values])
        where_clauses.append(f"{column} IN ({values_str})")
    
    # Add greater than or equal filters
    for column, value in geq_dict_filter.items():
        if isinstance(value, str):
            where_clauses.append(f"{column} >= '{value}'")
        else:
            where_clauses.append(f"{column} >= {value}")
    
    # Add less than or equal filters
    for column, value in leq_dict_filter.items():
        if isinstance(value, str):
            where_clauses.append(f"{column} <= '{value}'")
        else:
            where_clauses.append(f"{column} <= {value}")
    
    if where_clauses:
        where_clause_str = ' AND '.join(where_clauses)
        where_clause_sql = f"WHERE {where_clause_str}"
    else:
        where_clause_sql = ""

    sql_query = f"""
        SELECT {columns_str}
        FROM {schema}.{table_name}
        {where_clause_sql}
    """.strip()
    
    # Execute the query and return the result as a DataFrame
    with engine.begin() as connection:
        df = pd.read_sql(sql=text(sql_query), con=connection)
    
    return df

#engine = get_db_engine()


