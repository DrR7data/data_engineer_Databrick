CREATE OR REFRESH STREAMING TABLE RIDES_RAW 
COMMENT "Raw rides data streamed in from CSV files."
AS SELECT * FROM 
  STREAM READ_FILES("/Volumes/${catalog}/${schema}/raw_data/rides/*.csv", FORMAT => "csv");




CREATE STREAMING TABLE bronze.track (
    id INT COMMENT "Unique track identifier",
    title STRING COMMENT "Track title", 
    artist STRING COMMENT "Artist name",
    album STRING COMMENT "Album name", 
    album_id INTEGER COMMENT "Album ID reference" REFERENCES album(id),
    count INTEGER COMMENT "Play count", 
    rating INTEGER COMMENT "User rating", 
    len INTEGER COMMENT "Track length in seconds",
    PRIMARY KEY(id)
);

COPY INTO bronze.track
FROM '/Volumes/workspace/bronze/my-volume/library.csv'
FILEFORMAT = CSV;

SELECT * FROM bronze.track;