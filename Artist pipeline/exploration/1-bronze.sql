-- Bronze layer: Raw data ingestion from CSV files
-- CSV uses first data row as headers - mapping to proper column names
CREATE OR REFRESH STREAMING TABLE track (
  title STRING COMMENT "Track title",
  artist STRING COMMENT "Artist name",
  album STRING COMMENT "Album name",
  album_id INTEGER COMMENT "Album ID reference",
  count INTEGER COMMENT "Play count",
  rating INTEGER COMMENT "User rating",
  len INTEGER COMMENT "Track length in seconds"
)
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS SELECT 
  `Another One Bites The Dust` AS title,
  `Queen` AS artist,
  `Greatest Hits` AS album,
  `55` AS album_id,
  CAST(NULL AS INTEGER) AS count,
  `100` AS rating,
  `217` AS len
FROM STREAM(read_files(
  "/Volumes/library/create_pipeline_library/my_library",
  format => 'csv',
  header => true
));
