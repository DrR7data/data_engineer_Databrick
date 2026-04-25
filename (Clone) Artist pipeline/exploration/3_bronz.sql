-- Please edit the sample below

CREATE OR REFRESH MATERIALIZED VIEW 3_bronz AS
SELECT
    user_id,
    email,
    name,
    user_type
FROM samples.wanderbricks.users;