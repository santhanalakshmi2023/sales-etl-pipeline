import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)

products = [
    ("P001", "Laptop", "Electronics", 800, 1200),
    ("P002", "Mouse", "Electronics", 10, 30),
    ("P003", "Keyboard", "Electronics", 20, 60),
    ("P004", "Desk Chair", "Furniture", 100, 250),
    ("P005", "Monitor", "Electronics", 150, 350),
    ("P006", "Notebook", "Stationery", 2, 8),
    ("P007", "Pen Pack", "Stationery", 1, 5),
    ("P008", "Standing Desk", "Furniture", 200, 500),
    ("P009", "Headphones", "Electronics", 50, 150),
    ("P010", "Webcam", "Electronics", 30, 90),
]

regions = ["North", "South", "East", "West", "Central"]
sales_reps = ["Alice", "Bob", "Charlie", "Diana", "Ethan", "Fiona", "George", "Hannah"]
payment_methods = ["Credit Card", "UPI", "Net Banking", "Cash"]

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

start_date = datetime(2024, 1, 1)
end_date   = datetime(2024, 12, 31)

rows = []
for i in range(1, 1201):
    prod = random.choice(products)
    cost = prod[3]
    price = prod[4]
    qty = random.randint(1, 20)
    discount = round(random.uniform(0, 0.25), 2)
    revenue = round(price * qty * (1 - discount), 2)
    profit  = round((price - cost) * qty * (1 - discount), 2)

    # Inject ~5% nulls to make cleaning realistic
    rep = random.choice(sales_reps) if random.random() > 0.05 else None
    reg = random.choice(regions)    if random.random() > 0.05 else None

    rows.append({
        "sale_id":        f"S{i:05d}",
        "sale_date":      random_date(start_date, end_date).strftime("%Y-%m-%d"),
        "product_id":     prod[0],
        "product_name":   prod[1],
        "category":       prod[2],
        "region":         reg,
        "sales_rep":      rep,
        "quantity":       qty,
        "unit_price":     price,
        "unit_cost":      cost,
        "discount":       discount,
        "revenue":        revenue,
        "profit":         profit,
        "payment_method": random.choice(payment_methods),
    })

df = pd.DataFrame(rows)
df.to_csv("raw_sales.csv", index=False)
print(f"Generated {len(df)} rows -> raw_sales.csv")
print(df.head(3).to_string())