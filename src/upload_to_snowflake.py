import snowflake.connector
import pandas as pd
import os
import glob
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_USERNAME = os.getenv("SNOWFLAKE_USERNAME")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

def upload_to_snowflake():

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USERNAME,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        insecure_mode=True
    )

    cursor = conn.cursor()
    try:
            # Load the processed CSV
            csv_files = glob.glob('./data/processed_retail_sales_data/*.csv')
            csv_file_path =csv_files[0]
            
            # Save as new CSV if any modifications needed (optional)
            df = pd.read_csv(csv_file_path)
            processed_csv_path = './data/final_retail_sales_data.csv'
            df.to_csv(processed_csv_path, index=False)

            # Step 1: Upload the CSV to your internal stage 'my_stage'
            cursor.execute(f"PUT file://{os.path.abspath(processed_csv_path)} @my_stage AUTO_COMPRESS=TRUE")

            # Step 2: Copy data from the internal stage into the table
            cursor.execute("""
                COPY INTO RETAIL_SALES_DATA
                FROM @my_stage/final_retail_sales_data.csv.gz
                FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
                ON_ERROR = 'CONTINUE'
            """)

            print("Data uploaded to Snowflake successfully using COPY INTO with my_stage!")

    except Exception as e:
        print(f"Error during upload: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

upload_to_snowflake()