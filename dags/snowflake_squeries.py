# To successfully run the codes here using snowflake python operators, I have created snowflake aws connection 
# See aws_con_and_DDL file in snowflake folder to see these connections


#This query creates a partitioned table in snowflake
create_table_query = """ 
    CREATE OR REPLACE TABLE movie_raw(
        imdb VARCHAR,
        title VARCHAR,
        rating INT,
        year DATE,
        runtime INT,
        top250 INT,
        top250tv INT, 
        title_date DATE
    )
    cluster by (title_date);
"""

# This query loads data from the snowflake stage into the partitioned table
load_title_query = """ 
    COPY INTO movie_raw
    FROM @movie_title_stage
    FILE_FORMAT = csv_format
    on_error = continue;
"""


