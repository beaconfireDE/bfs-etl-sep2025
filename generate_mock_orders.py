import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# === CONFIG ===
GROUP_NUM = "team1" 
BASE_DATE = datetime(2025, 10, 24)
NUM_RECORDS = 100 
OUT_DIR = "./data"  
TABLE_NAME = "orders"

# === FUNCTION TO GENERATE RANDOM DATA ===
def generate_mock_data(date_str):
    np.random.seed()
    data = {
        "order_id": np.arange(1, NUM_RECORDS + 1),
        "customer_id": np.random.randint(1000, 9999, NUM_RECORDS),
        "order_date": [date_str] * NUM_RECORDS,
        "product_id": np.random.randint(100, 200, NUM_RECORDS),
        "product_name": np.random.choice(
            ["Laptop", "Phone", "Monitor", "Mouse", "Keyboard", "Headset", "Camera"], NUM_RECORDS),
        "qty": np.random.randint(1, 5, NUM_RECORDS),
        "price": np.round(np.random.uniform(10, 1000, NUM_RECORDS), 2),
        "currency": np.random.choice(["USD", "EUR", "JPY"], NUM_RECORDS),
        "sales_channel": np.random.choice(["Online", "Retail", "Wholesale"], NUM_RECORDS),
        "region": np.random.choice(["North", "South", "East", "West"], NUM_RECORDS),
    }

    df = pd.DataFrame(data)
    filename = f"{TABLE_NAME}_{GROUP_NUM}_{date_str}.csv"
    df.to_csv(f"{OUT_DIR}/{filename}", index=False)
    print(f"Generated {filename} with {NUM_RECORDS} rows and {len(df.columns)} columns")

# === GENERATE 3-day order files ===
for i in range(3):
    date_str = (BASE_DATE + timedelta(days=i)).strftime("%Y-%m-%d")
    generate_mock_data(date_str)
