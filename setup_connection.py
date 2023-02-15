import snowflake.connector
import csv
exportConnection = snowflake.connector.connect(
    user='Sarayu',
    password='Lisnepal123!',
    account='yc85141.central-india.azure',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN',
    database='BHATBHATENI',
    schema='TRANSACTIONS'
)
exportCursor = exportConnection.cursor()

bhatbhateniWHConnection = snowflake.connector.connect(
    user='Sarayu',
    password='Lisnepal123!',
    account='yc85141.central-india.azure',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN',
)
bhatbhateniWHCursor = bhatbhateniWHConnection.cursor()
