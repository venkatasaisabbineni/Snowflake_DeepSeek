import snowflake.connector
import pandas as pd
import httpx
import json
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_USERNAME = os.getenv("SNOWFLAKE_USERNAME")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")


def analyze_with_deepseek():
    # Connect to Snowflake
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
    # Fetch data
    query = "SELECT * FROM RETAIL_SALES_DATA"
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    print(f"Fetched {len(df)} rows from Snowflake")
    conn.close()

    print("Columns in the Data Frame : ",df.columns.tolist())

    aggregated_data = {
            "total_retail_sales_by_item_type": df.groupby("ITEM_TYPE")["RETAIL_SALES"].sum().to_dict(),
            "total_retail_sales_by_year": df.groupby("YEAR")["RETAIL_SALES"].sum().to_dict(),
            "top_5_items_by_sales": df.groupby("ITEM_DESCRIPTION")["RETAIL_SALES"].sum().nlargest(5).to_dict(),
            "monthly_sales_trend": df.groupby(["YEAR", "MONTH"])["RETAIL_SALES"].sum().reset_index().to_dict("records"),
            "retail_sales_by_supplier": df.groupby("SUPPLIER")["RETAIL_SALES"].sum().to_dict(),
            "retail_transfers_by_item_type": df.groupby("ITEM_TYPE")["RETAIL_TRANSFERS"].sum().to_dict(),
            "warehouse_vs_retail_sales": df.groupby("ITEM_TYPE")[["WAREHOUSE_SALES", "RETAIL_SALES"]].sum().to_dict(),
            "seasonal_trends": df.groupby("MONTH")["RETAIL_SALES"].mean().to_dict(),
            "supplier_performance": df.groupby("SUPPLIER")[["RETAIL_SALES", "WAREHOUSE_SALES"]].sum().to_dict(),
        }

    # Sample of raw data for context
    data_sample = df.head(100).to_csv(index=False)

    # Create prompt
    prompt = f"""
    I have a dataset with the following columns: {', '.join(df.columns)}.
    Here is a summary of the data:
    - Total retail sales by item type: {aggregated_data['total_retail_sales_by_item_type']}
    - Total retail sales by year: {aggregated_data['total_retail_sales_by_year']}
    - Top 5 items by sales: {aggregated_data['top_5_items_by_sales']}
    - Monthly sales trend: {aggregated_data['monthly_sales_trend']}
    - Retail sales by supplier: {aggregated_data['retail_sales_by_supplier']}
    - Retail transfers by item type: {aggregated_data['retail_transfers_by_item_type']}
    - Warehouse vs. retail sales: {aggregated_data['warehouse_vs_retail_sales']}
    - Seasonal trends: {aggregated_data['seasonal_trends']}
    - Supplier performance: {aggregated_data['supplier_performance']}

    Here is a sample of the raw data for context:
    {data_sample}
    Can you answer the following questions:
    Which supplier sells the most units by retail sales?
    What is the percentage of total sales contributed by each supplier?
    Which suppliers are consistently transferring products to the warehouse, even if their retail sales are low?
    How do suppliers perform in different months and years? Are there any trends in their sales patterns?
    """

    # Call Deepseek API
    client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")

    messages = [{"role": "user", "content": prompt}]
    try:
        response = client.chat.completions.create(
            model="deepseek-reasoner",
            messages=messages
        )
        print("API Response:", response)
        print("Generated Content:", response.choices[0].message.content)
        output_file_path = "./output/deepseek_analysis_output2.txt"
        with open(output_file_path, "w") as f:
            f.write(response.choices[0].message.content)
        print(f"Analysis results saved to {output_file_path}")
    except httpx.HTTPStatusError as e:
        print("HTTP Error:", e.response.status_code)
        print("Response Content:", e.response.text)
    except json.JSONDecodeError as e:
        print("JSON Decode Error:", str(e))
    except Exception as e:
        print("Unexpected Error:", str(e))

analyze_with_deepseek()


