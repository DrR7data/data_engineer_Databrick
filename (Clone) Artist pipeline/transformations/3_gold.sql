-- Please edit the sample below

USE CATALOG library;
use schema create_pipeline_library;

CREATE OR REPLACE MATERIALIZED VIEW track_gold AS
SELECT 
t.title AS track, 
a.title AS album,
ar.name as artist
FROM track AS t
JOIN album AS a ON t.album_id = a.id
JOIN tracktoartist AS ta ON t.id = ta.track_id
JOIN artist AS ar ON ta.artist_id = ar.id