-- Please edit the sample below

USE CATALOG library;
use schema create_pipeline_library;

/*CREATE OR REPLACE MATERIALIZED VIEW album
AS
SELECT DISTINCT(_c3) AS title
FROM track_raw;*/

CREATE OR REPLACE MATERIALIZED VIEW album
    WITH cte_album AS 
    (SELECT DISTINCT(_c2) AS title
    FROM  track_raw) 
SELECT
ROW_NUMBER() OVER (ORDER BY title DESC) as id, 
* FROM cte_album;

/*CREATE OR REPLACE MATERIALIZED VIEW artist
AS
SELECT DISTINCT(_c2) AS name
FROM track_raw;*/


CREATE OR REPLACE MATERIALIZED VIEW artist
    WITH cte_artist AS 
    (SELECT DISTINCT(_c1) AS name
    FROM  track_raw) 
SELECT
ROW_NUMBER() OVER (ORDER BY name DESC) as id, 
* FROM cte_artist;


CREATE OR REPLACE MATERIALIZED VIEW track
WITH cte_track AS 
(SELECT 
_c0 AS title,
--_c1 AS artist,
--_c2 As album,
_c3 as count,
_c4 AS routing,
_c5 AS len,
a.id AS album_id,
ar.id As artist_id
FROM  track_raw t
Join album AS a ON t._c2 = a.title
join artist As ar ON t._c1 = ar.name) 
SELECT
ROW_NUMBER() OVER (ORDER BY title DESC) as id, 
* FROM cte_track;

CREATE OR REPLACE MATERIALIZED VIEW tracktoartist
AS
SELECT 
t.id AS track_id,
a.id AS artist_id
FROM track AS t
JOIN artist AS a ON t.artist_id = a.id;



