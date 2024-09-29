import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
from google.cloud import bigquery

# Initialize Faker and set random seed for reproducibility
fake = Faker()
random.seed(42)
np.random.seed(42)


from google.oauth2 import service_account
import json


def get_credentials():
    credentials = service_account.Credentials.from_service_account_file('C:/Users/daniel.watanabe_ingr/Documents/dbt-study/dbt-env/dbt_sales_proj/sa.json')
    return credentials

def generate_client():
    credentials = get_credentials()
    client = bigquery.Client(credentials=credentials)
    return client

client = generate_client()

# Your BigQuery dataset name
dataset_name = 'dbt_sales_proj_raw'  # Change if your dataset name is different

# Your GCP project ID
project_id = client.project

# Current timestamp for created_at and updated_at
current_timestamp = datetime.utcnow()

# List of possible currencies
currencies = ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD']

# List of possible order statuses (Updated)
status_list = [
    {'id': 1, 'status_name': 'Declined'},
    {'id': 2, 'status_name': 'Pending Approval'},
    {'id': 3, 'status_name': 'Approved'},
    {'id': 4, 'status_name': 'Sent'},
    {'id': 5, 'status_name': 'Delivered'},
    {'id': 6, 'status_name': 'Refunded'}
]

# List of possible payment options
payment_options_list = [
    {'id': 1, 'payment_method': 'Credit Card'},
    {'id': 2, 'payment_method': 'Debit Card'},
    {'id': 3, 'payment_method': 'PayPal'},
    {'id': 4, 'payment_method': 'Bank Transfer'},
    {'id': 5, 'payment_method': 'Cash on Delivery'}
]

# List of possible product categories and sample products (unchanged)
product_categories = {
    'Electronics': ['Smartphone', 'Laptop', 'Headphones', 'Smartwatch', 'Camera'],
    'Home Appliances': ['Refrigerator', 'Microwave', 'Air Conditioner', 'Washing Machine', 'Vacuum Cleaner'],
    'Books': ['Novel', 'Biography', 'Science Fiction', 'Non-Fiction', 'Fantasy'],
    'Clothing': ['T-Shirt', 'Jeans', 'Dress', 'Jacket', 'Sweater'],
    'Sports': ['Football', 'Basketball', 'Tennis Racket', 'Yoga Mat', 'Dumbbells']
}

# Generate Status Table
status_df = pd.DataFrame(status_list)
status_df['created_at'] = current_timestamp
status_df['updated_at'] = current_timestamp

# Generate Payment Option Table
payment_options_df = pd.DataFrame(payment_options_list)
payment_options_df['created_at'] = current_timestamp
payment_options_df['updated_at'] = current_timestamp

# Generate Users Table (includes birthdate)
number_of_users = 100
users = []

for i in range(1, number_of_users + 1):
    # Generate a birthdate between 18 and 70 years ago
    birthdate = fake.date_of_birth(minimum_age=18, maximum_age=70)
    user = {
        'id': i,
        'name': fake.name(),
        'email': fake.email(),
        'address': fake.address().replace('\n', ', '),
        'birthdate': birthdate,
        'created_at': current_timestamp,
        'updated_at': current_timestamp
    }
    users.append(user)

users_df = pd.DataFrame(users)

# Generate Seller Conditions Table
number_of_sellers = 20
sellers = []

for i in range(1, number_of_sellers + 1):
    seller_currency = random.choice(currencies)
    seller = {
        'id': i,
        'seller_name': fake.company(),
        'fee_percentage': round(random.uniform(0.05, 0.20), 4),  # Fee between 5% and 20%
        'currency': seller_currency,
        'days_until_liquidation': random.randint(1, 30),  # Number of days until liquidation
        'created_at': current_timestamp,
        'updated_at': current_timestamp
    }
    sellers.append(seller)

sellers_df = pd.DataFrame(sellers)

# Generate Products Table (includes category and realistic product names)
number_of_products = 200
products = []

category_list = list(product_categories.keys())

for i in range(1, number_of_products + 1):
    seller_id = random.randint(1, number_of_sellers)
    category = random.choice(category_list)
    product_name = random.choice(product_categories[category])
    # Append a random adjective to the product name for variety
    product_name = f"{fake.word().title()} {product_name}"
    product = {
        'id': i,
        'product_name': product_name,
        'category': category,
        'seller_id': seller_id,
        'price': round(random.uniform(10, 500), 2),
        'created_at': current_timestamp,
        'updated_at': current_timestamp
    }
    products.append(product)

products_df = pd.DataFrame(products)

# Generate Orders Table
number_of_orders = 500
orders = []

for i in range(1, number_of_orders + 1):
    status_id = random.randint(1, len(status_list))
    payment_option_id = random.randint(1, len(payment_options_list))
    order_date = fake.date_between(start_date='-1y', end_date='today')
    order = {
        'id': i,
        'user_id': random.randint(1, number_of_users),
        'order_date': order_date,
        'installments': random.randint(1, 12),  # Installments between 1 and 12
        'status_id': status_id,
        'payment_option_id': payment_option_id,
        'created_at': current_timestamp,
        'updated_at': current_timestamp
    }
    orders.append(order)

orders_df = pd.DataFrame(orders)

# Generate Order Items Table
order_items = []
order_item_id = 1

for order in orders:
    num_items = random.randint(1, 5)  # Each order has 1 to 5 items
    items = random.sample(range(1, number_of_products + 1), num_items)
    for product_id in items:
        product = products_df[products_df['id'] == product_id].iloc[0]
        seller_id = product['seller_id']
        price = product['price']
        # Get the fee_percentage from seller_conditions
        fee_percentage = sellers_df[sellers_df['id'] == seller_id]['fee_percentage'].iloc[0]
        fee_value = round(price * fee_percentage, 2)
        order_item = {
            'id': order_item_id,
            'order_id': order['id'],
            'product_id': product_id,
            'seller_id': seller_id,
            'price': price,
            'fee_value': fee_value,
            'created_at': current_timestamp,
            'updated_at': current_timestamp
        }
        order_items.append(order_item)
        order_item_id += 1

order_items_df = pd.DataFrame(order_items)

# Calculate products_value and fee_value per order
products_value_df = order_items_df.groupby('order_id')['price'].sum().reset_index()
products_value_df.rename(columns={'price': 'products_value'}, inplace=True)

fee_value_df = order_items_df.groupby('order_id')['fee_value'].sum().reset_index()
fee_value_df.rename(columns={'fee_value': 'fee_value'}, inplace=True)

# Merge into orders_df
orders_df = pd.merge(orders_df, products_value_df, left_on='id', right_on='order_id', how='left')
orders_df = pd.merge(orders_df, fee_value_df, left_on='id', right_on='order_id', how='left')

# Function to calculate interest_fee
def calculate_interest_fee(installments, products_value):
    if installments == 1:
        interest_rate = 0
    elif installments <= 3:
        interest_rate = (installments - 1) * 0.01
    elif installments <=6:
        interest_rate = (installments - 1) * 0.015
    else:
        interest_rate = (installments - 1) * 0.02
    interest_fee = round(products_value * interest_rate, 2)
    return interest_fee

# Now calculate interest_fee
orders_df['interest_fee'] = orders_df.apply(lambda row: calculate_interest_fee(row['installments'], row['products_value']), axis=1)

# Calculate total_transaction_value
orders_df['total_transaction_value'] = orders_df['products_value'] + orders_df['fee_value'] + orders_df['interest_fee']

# Drop unnecessary columns
orders_df.drop(columns=['order_id_x', 'order_id_y'], inplace=True, errors='ignore')

# Optional: Reorder Columns for Clarity
orders_df = orders_df[['id', 'user_id', 'order_date', 'installments', 'status_id', 'payment_option_id',
                       'products_value', 'fee_value', 'interest_fee', 'total_transaction_value',
                       'created_at', 'updated_at']]

# Prepare the Currency Exchange Rates Table
# Get the date range for which we need exchange rates
order_dates = pd.to_datetime(orders_df['order_date'])
start_date = order_dates.min()
end_date = order_dates.max()

date_range = pd.date_range(start=start_date, end=end_date, freq='D')

# Generate mock exchange rates for each currency on each date
exchange_rates_list = []

for single_date in date_range:
    for currency in currencies:
        if currency == 'USD':
            rate = 1.0  # USD is the base currency
        else:
            # Generate a random exchange rate for the currency on that date
            rate = round(random.uniform(0.5, 1.5), 4)
        exchange_rate = {
            'date': single_date.date(),
            'currency': currency,
            'exchange_rate_to_usd': rate
        }
        exchange_rates_list.append(exchange_rate)

currency_exchange_rates_df = pd.DataFrame(exchange_rates_list)

# Generate Ratings Table
ratings = []
rating_id = 1

for order_item in order_items:
    # Randomly decide if a rating was given (e.g., 70% chance)
    if random.random() < 0.7:
        rating = {
            'id': rating_id,
            'order_id': order_item['order_id'],
            'order_item_id': order_item['id'],
            'rating': random.randint(1, 5),
            'comment': fake.sentence(nb_words=10),
            'created_at': current_timestamp,
            'updated_at': current_timestamp,
            'deleted_at': None  # Assuming no deletions initially
        }
        ratings.append(rating)
        rating_id += 1

ratings_df = pd.DataFrame(ratings)

# Function to upload DataFrame to BigQuery
def upload_to_bigquery(df, table_name):
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    # Convert date columns to strings if necessary
    # BigQuery can handle pandas datetime objects, but ensure they're in correct format
    if 'order_date' in df.columns:
        df['order_date'] = pd.to_datetime(df['order_date']).dt.date
    if 'birthdate' in df.columns:
        df['birthdate'] = pd.to_datetime(df['birthdate']).dt.date
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date']).dt.date
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'])
    if 'updated_at' in df.columns:
        df['updated_at'] = pd.to_datetime(df['updated_at'])
    if 'deleted_at' in df.columns:
        df['deleted_at'] = pd.to_datetime(df['deleted_at'])

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()  # Wait for the job to complete

    print(f"Loaded {len(df)} rows into {table_id}.")

# Upload DataFrames to BigQuery
upload_to_bigquery(users_df, 'users')
upload_to_bigquery(sellers_df, 'seller_conditions')
upload_to_bigquery(products_df, 'products')
upload_to_bigquery(orders_df, 'orders')
upload_to_bigquery(order_items_df, 'order_items')
upload_to_bigquery(currency_exchange_rates_df, 'currency_exchange_rates')
upload_to_bigquery(status_df, 'status')
upload_to_bigquery(payment_options_df, 'payment_option')
upload_to_bigquery(ratings_df, 'ratings')