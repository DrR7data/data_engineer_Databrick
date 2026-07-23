import os
from dotenv import load_dotenv


AWS_DELTA_LAKE_ITER = 's3://databric-rrr/iter_20'
AWS_DELTA_LAKE_ITER_BRONZE = f"{AWS_DELTA_LAKE_ITER}/bronze_i"
AWS_DELTA_LAKE_ITER_SILVER = f"{AWS_DELTA_LAKE_ITER}/silver_i"
AWS_DELTA_LAKE_ITER_GOLD = f"{AWS_DELTA_LAKE_ITER}/gold_i"