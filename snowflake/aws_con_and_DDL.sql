-- Creating file format for csv files
CREATE OR REPLACE file format csv_format
    type = csv 
    field_delimiter = ','
    skip_header = 1
    empty_field_as_null = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE; 

-- creating storage integration to connect aws to snowflake
create or replace storage integration s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE 
    STORAGE_AWS_ROLE_ARN = '{Role arn here}' 
    STORAGE_ALLOWED_LOCATIONS = ('s3://myde-tf-bucket/titles/', 's3://myde-tf-bucket/')
    COMMENT = 'creating integration connection with s3';

-- run this code to get snowflake arn and update in the AWS role trust policy
DESC integration s3_int;

-- Creating  stage to hold data
CREATE OR REPLACE STAGE movie_title_stage
    URL = 's3://myde-tf-bucket/titles/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = csv_format;

-- test s3 bucket connection
list @movie_title_stage;




