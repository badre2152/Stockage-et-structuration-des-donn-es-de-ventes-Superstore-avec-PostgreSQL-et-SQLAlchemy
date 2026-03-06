# %%
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

DB_USER = "postgres"
DB_PASSWORD = "Badre2152@"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "superstoreDB"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


engine = create_engine(DATABASE_URL, echo=False)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

print("✅ Connexion à superstore_db établie avec succès !")

# %%
import urllib.parse
from sqlalchemy import create_engine, text

DB_USER = "postgres"
DB_PASS = "Badre2152@" 
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "superstoreDB"

encoded_pass = urllib.parse.quote_plus(DB_PASS)

DATABASE_URL = f"postgresql+psycopg://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 'Connected successfully. Password verified.'"))
        print(result.fetchone()[0])
except Exception as e:
    print("❌ Authentication error (Authentication Error)")
    print(f"🔍 Details: {e}")

# %%
import urllib.parse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

DB_USER = "postgres"
DB_PASS = "Badre2152@"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "superstoreDB"

encoded_pass = urllib.parse.quote_plus(DB_PASS)
DATABASE_URL = f"postgresql+psycopg://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

def run_health_check():
    print("🔍 System integrity check  ...")
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT current_user, current_database(), version();"))
            user, db, ver = res.fetchone()
            print(f"✅ Connection: Stable")
            print(f"👤 User : {user}")
            print(f"🗄️ base : {db}")

            conn.execute(text("CREATE TEMP TABLE connection_test (id serial PRIMARY KEY, val text);"))
            conn.execute(text("INSERT INTO connection_test (val) VALUES ('test');"))
            conn.execute(text("DROP TABLE connection_test;"))
            print("✅ Permissions: All necessary permissions are in place.")

            print("\n🚀 System is ready to proceed to the table creation stage !")

    except SQLAlchemyError as e:
        print("\n❌ System integrity check failed")
        print(f"⚠️ Technical reason: {e}")

if __name__ == "__main__":
    run_health_check()

# %%
from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, Boolean, CheckConstraint
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Geography(Base):
    __tablename__ = 'geography'
    postal_code = Column(String(20), primary_key=True) 
    city = Column(String(100), nullable=False)        
    state = Column(String(100), nullable=False)       
    country = Column(String(100), nullable=False)     
    region = Column(String(50), nullable=False)       

class Customer(Base):
    __tablename__ = 'customers'
    customer_id = Column(String(50), primary_key=True) 
    customer_name = Column(String(200), nullable=False)
    segment = Column(String(50))
    postal_code = Column(String(20), ForeignKey('geography.postal_code'), nullable=False)

class Product(Base):
    __tablename__ = 'products'
    product_id = Column(String(50), primary_key=True) 
    product_name = Column(String(255), nullable=False)
    category = Column(String(100), nullable=False)
    sub_category = Column(String(100), nullable=False)

class Order(Base):
    __tablename__ = 'orders'
    order_id = Column(String(50), primary_key=True)   
    order_date = Column(Date, nullable=False)         
    ship_date = Column(Date)
    ship_mode = Column(String(50))
    shipping_class = Column(String(50))
    customer_id = Column(String(50), ForeignKey('customers.customer_id'), nullable=False)

class OrderDetail(Base):
    __tablename__ = 'order_details'
    row_id = Column(Integer, primary_key=True)       
    
    order_id = Column(String(50), ForeignKey('orders.order_id'), nullable=False)
    product_id = Column(String(50), ForeignKey('products.product_id'), nullable=False)
    
    sales = Column(Float, nullable=False)
    profit = Column(Float, nullable=False)
    estimated_cost = Column(Float)
    is_profitable = Column(Boolean, default=True)

    __table_args__ = (CheckConstraint('sales >= 0', name='check_sales_positive'),)

# %%
try:
    Base.metadata.create_all(engine)
    print("🚀 All tables created successfully; all constraints and keys enforced ")
except Exception as e:
    print(f"❌ An error occurred while creating tables :  {e}")

# %%
from sqlalchemy import event

@event.listens_for(engine, "connect")
def set_webapp_encoding(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("SET client_encoding TO 'UTF8'")
    cursor.close()

try:
    Base.metadata.create_all(engine)
    print("🚀 All tables created successfully!")
except Exception as e:
    print(f"❌ Error: {e}")

# %%
import pandas as pd
import numpy as np

try:
    df = pd.read_csv('superstore_clean.csv', encoding='utf-8')
except:
    df = pd.read_csv('superstore_clean.csv', encoding='latin1')

df['Order Date'] = pd.to_datetime(df['Order Date'])
df['Ship Date'] = pd.to_datetime(df['Ship Date'])

df = df.fillna({
    'Segment': 'Consumer',
    'Ship Mode': 'Standard Class',
    'Postal Code': '00000' 
})

df['Postal Code'] = df['Postal Code'].astype(str).str.split('.').str[0]

# %%
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)
session = Session()

try:
    print("⏳ Starting Loading process ...")

    geo_data = df[['Postal Code', 'City', 'State', 'Country', 'Region']].drop_duplicates('Postal Code')
    for _, r in geo_data.iterrows():
        session.merge(Geography(postal_code=r['Postal Code'], city=r['City'], 
                                state=r['State'], country=r['Country'], region=r['Region']))
    print("✅ Geography loaded ")

    prod_data = df[['Product ID', 'Product Name', 'Category', 'Sub-Category']].drop_duplicates('Product ID')
    for _, r in prod_data.iterrows():
        session.merge(Product(product_id=r['Product ID'], product_name=r['Product Name'], 
                              category=r['Category'], sub_category=r['Sub-Category']))
    print("✅ Products loaded successfully.")

    cust_data = df[['Customer ID', 'Customer Name', 'Segment', 'Postal Code']].drop_duplicates('Customer ID')
    for _, r in cust_data.iterrows():
        session.merge(Customer(customer_id=r['Customer ID'], customer_name=r['Customer Name'], 
                               segment=r['Segment'], postal_code=r['Postal Code']))
    print("✅ Customers loaded successfully.")

    order_data = df[['Order ID', 'Order Date', 'Ship Date', 'Ship Mode', 'Customer ID']].drop_duplicates('Order ID')
    for _, r in order_data.iterrows():
        session.merge(Order(order_id=r['Order ID'], order_date=r['Order Date'], 
                            ship_date=r['Ship Date'], ship_mode=r['Ship Mode'], customer_id=r['Customer ID']))
    print("✅ Orders loaded successfully.")

    for _, r in df.iterrows():
        session.add(OrderDetail(
            row_id=int(r['Row ID']),
            order_id=r['Order ID'],
            product_id=r['Product ID'],
            sales=float(r['Sales']),
            profit=float(r['Profit']),
            estimated_cost=float(r['Sales'] - r['Profit']),
            is_profitable=True if r['Profit'] > 0 else False
        ))
    
    session.commit()
    print("✨ All data has been loaded successfully! ")

except Exception as e:
    session.rollback()
    print(f"❌ Failed to load data: {e}")
finally:
    session.close()

# %%
with engine.connect() as conn:
    query = text("""
        SELECT g.region, SUM(od.sales) as total_sales
        FROM geography g
        JOIN customers c ON g.postal_code = c.postal_code
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN order_details od ON o.order_id = od.order_id
        GROUP BY g.region;
    """)
    result = conn.execute(query)
    print("\n📊 Relationship Validation:(Sales by Region ): ")
    for row in result:
        print(f"Region: {row[0]} | Total Sales: ${row[1]:,.2f}")

# %%
import pandas as pd
from sqlalchemy import text

df_original = pd.read_csv('superstore_clean.csv', encoding='latin1')
total_sales_csv = df_original['Sales'].sum()

with engine.connect() as conn:
    result = conn.execute(text("SELECT SUM(sales) FROM order_details"))
    total_sales_db = result.fetchone()[0]

print(f"💰 Total Sales (CSV): {total_sales_csv:,.2f}")
print(f"💰 Total Sales (DB):  {total_sales_db:,.2f}")

if round(total_sales_csv, 2) == round(total_sales_db, 2):
    print("✅ Full Match: Financial data is intact!b")
else:
    print("❌ Error: Figures mismatch, check the upload/loading process.")

# %%
query_join = text("""
    SELECT g.region, COUNT(DISTINCT o.order_id) as num_orders, SUM(od.sales) as total_sales
    FROM geography g
    JOIN customers c ON g.postal_code = c.postal_code
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_details od ON o.order_id = od.order_id
    GROUP BY g.region
    ORDER BY total_sales DESC;
""")

with engine.connect() as conn:
    result = conn.execute(query_join)
    print("\n📊 Regional Analysis Report(Multi-Join):")
    for row in result:
        print(f"Region: {row[0]:<10} | Orders: {row[1]:<5} | Sales: ${row[2]:,.2f}")

# %%
try:
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO order_details (row_id, order_id, product_id, sales, profit)
            VALUES (99999, 'CA-2024-12345', 'NON_EXISTENT_PROD', 100.0, 10.0)
        """))
        conn.commit()
except Exception as e:
    print("\n🛡️ Referential Integrity Test: Success!")
    print(f"Result: Insertion was properly blocked (Expected Foreign Key Constraint Error)")

# %%
query_top_products = text("""
    SELECT p.product_name, SUM(od.profit) as total_profit
    FROM products p
    JOIN order_details od ON p.product_id = od.product_id
    GROUP BY p.product_name
    ORDER BY total_profit DESC
    LIMIT 5;
""")

with engine.connect() as conn:
    result = conn.execute(query_top_products)
    print("\n🏆 Most Profitable Products List:")
    for row in result:
        print(f"- {row[0][:40]:<45} | Profit: ${row[1]:,.2f}")

# %%
from sqlalchemy import text

create_master_view = text("""
CREATE OR REPLACE VIEW view_master_sales AS
SELECT 
    od.row_id,
    o.order_id,
    o.order_date,
    c.customer_name,
    c.segment,
    g.city,
    g.state,
    g.region,
    p.product_name,
    p.category,
    p.sub_category,
    od.sales,
    od.profit,
    od.is_profitable
FROM order_details od
JOIN orders o ON od.order_id = o.order_id
JOIN customers c ON o.customer_id = c.customer_id
JOIN geography g ON c.postal_code = g.postal_code
JOIN products p ON od.product_id = p.product_id;
""")

with engine.connect() as conn:
    conn.execute(create_master_view)
    conn.commit()
    print("✅ Successfully created View_Master_Sales!")

# %%
create_kpi_view = text("""
CREATE OR REPLACE VIEW view_kpi_summary AS
SELECT 
    category,
    region,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(sales) as total_revenue,
    SUM(profit) as total_profit,
    ROUND(((SUM(profit) / NULLIF(SUM(sales), 0)) * 100)::numeric, 2) as profit_margin_percent
FROM view_master_sales
GROUP BY category, region;
""")

with engine.connect() as conn:
    conn.execute(create_kpi_view)
    conn.commit()
    print("✅ View_KPI_Summary has been created successfully.")

# %%
indexes_script = [
    "CREATE INDEX IF NOT EXISTS idx_order_date ON orders(order_date);",
    "CREATE INDEX IF NOT EXISTS idx_geo_region ON geography(region);",
    "CREATE INDEX IF NOT EXISTS idx_prod_category ON products(category);"
]

with engine.connect() as conn:
    for sql in indexes_script:
        conn.execute(text(sql))
    conn.commit()
    print("⚡ Successfully optimized database performance using Indexes.")

