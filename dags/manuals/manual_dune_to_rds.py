# import os
# import pandas as pd
# from sqlalchemy import create_engine
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.hooks.base import BaseHook
# from airflow.models import Variable
# from datetime import datetime, timedelta
# from dune_client.client import DuneClient
# from dune_client.query import QueryBase, QueryParameter

# # ------------------------------------------------------------------------------------
# # Settings
# # ------------------------------------------------------------------------------------

# TABLE_NAMES = [
#     "dune.sc_research.aptos_collateral_value",
#     "dune.sc_research.aptos_staking",
#     "dune.sc_research.avalanche_collateral_value",
#     "dune.sc_research.avalanche_staking",
#     "dune.sc_research.binance_collateral_value",
#     "dune.sc_research.binance_staking",
#     "dune.sc_research.bittensor_collateral_value",
#     "dune.sc_research.bittensor_marketcap_retro",
#     "dune.sc_research.bittensor_network_stats",
#     "dune.sc_research.bittensor_staking",
#     "dune.sc_research.bittensor_subnets_stats",
#     "dune.sc_research.celestia_collateral_value",
#     "dune.sc_research.celestia_staking",
#     "dune.sc_research.ethereum_collateral_value",
#     "dune.sc_research.ethereum_staking",
#     "dune.sc_research.msov_asset_metrics",
#     "dune.sc_research.near_collateral_value",
#     "dune.sc_research.near_staking",
#     "dune.sc_research.sei_collateral_value",
#     "dune.sc_research.sei_staking",
#     "dune.sc_research.solana_collateral_value",
#     "dune.sc_research.solana_staking",
#     "dune.sc_research.sui_collateral_value",
#     "dune.sc_research.sui_staking",
#     "dune.sc_research.ton_collateral_value",
#     "dune.sc_research.ton_staking",
#     ["query_4504144", "prices_usd_daily"],
# ]

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1)
# }

# dag = DAG(
#     'manual_dune_to_rds',
#     default_args=default_args,
#     description='Pull data from Dune tables and upload to RDS',
#     schedule_interval=timedelta(days=10000),
#     start_date=datetime(2024, 1, 1),
#     catchup=False
# )

# # ------------------------------------------------------------------------------------
# # Functions
# # ------------------------------------------------------------------------------------

# def fetch_and_upload_to_rds(dune_table_name, rds_table_name):
#     """
#     Fetches data from Dune and uploads it to RDS.
#     """
#     try:
#         # Load the Dune API key
#         api_key = Variable.get("api_key_dune")

#         # Initialize the DuneClient with free performance
#         dune = DuneClient(api_key=api_key, performance='medium')

#         # Fetch data from Dune
#         query = QueryBase(name=f"Fetch_{dune_table_name}", query_id=4504019, params=[QueryParameter.text_type(name='table', value=dune_table_name)])
#         results = dune.run_query(query)
#         df = pd.DataFrame(results.result.rows)

#         # Initialize RDS connection
#         rds_conn = BaseHook.get_connection('rds_connection')
#         rds_engine = create_engine(
#             f"postgresql://{rds_conn.login}:{rds_conn.password}@{rds_conn.host}:{rds_conn.port}/{rds_conn.schema}"
#         )

#         # Upload data to RDS
#         df.to_sql(rds_table_name, rds_engine, if_exists='append', index=False)
#         print(f"Uploaded {len(df)} rows to RDS table {rds_table_name}")

#     except Exception as e:
#         print(f"Error processing table {rds_table_name}: {e}")
#         raise

# # ------------------------------------------------------------------------------------
# # DAG Tasks
# # ------------------------------------------------------------------------------------

# for table_name in TABLE_NAMES:
#     if type(table_name) == list:
#         dune_table_name = table_name[0]
#         rds_table_name = table_name[1]
#     elif type(table_name) == str:
#         dune_table_name = table_name
#         rds_table_name = table_name.replace("dune.sc_research.", "")
#     else:
#         raise ValueError(f"Invalid table_name: {table_name}")

#     task = PythonOperator(
#         task_id=f'fetch_and_upload_{rds_table_name.replace(".", "_")}',
#         python_callable=fetch_and_upload_to_rds,
#         op_kwargs={'dune_table_name': dune_table_name, 'rds_table_name': rds_table_name},
#         dag=dag
#     )