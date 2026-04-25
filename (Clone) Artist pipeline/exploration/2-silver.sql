-- Please edit the sample below

CREATE MATERIALIZED VIEW 2_silver AS
SELECT
    user_id,
    email,
    name,
    user_type
FROM samples.wanderbricks.users;

COPY INTO silver.track(title,artist,album,count,rating,len)
FROM '/Volumes/workspace/bronze/my-volume/library/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');