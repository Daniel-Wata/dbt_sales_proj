
# DBT Sales Project

## Idea
Use fake data to work with dbt models in conjunction with a bigquery data warehouse, the purpose is to have raw, bronze, silver and gold layers and process the data among the layers using dbt.

## The data
I designed a relational database of sales with some basic tables, the data itself was generated and uploaded to the raw layer using the data_uploader.py file (I explained the tables and relationships to chatGPT and let him do the script =)).

### Tables and Relationships Explanation

- **users**
  - **id**: Primary key.
  - **name**, **email**, **address**: User details.
  - **Relationship**: Each user can place multiple orders.

- **seller_conditions**
  - **id**: Primary key.
  - **seller_name**, **fee_percentage**, **currency**, **days_until_liquidation**: Seller details and conditions.
  - **Relationship**:
    - Offers multiple products.
    - Sells items in order items.
    - Associated with currency exchange rates based on the currency.

- **products**
  - **id**: Primary key.
  - **product_name**, **seller_id**, **price**: Product details.
  - **Relationship**:
    - Each product is offered by one seller (seller_conditions).
    - A product can be part of many order items.

- **orders**
  - **id**: Primary key.
  - **user_id**, **order_date**, **installments**, **products_value**, **fee_value**, **interest_fee**, **total_transaction_value**: Order details.
  - **Relationship**:
    - Placed by one user.
    - Contains multiple order items.

- **order_items**
  - **id**: Primary key.
  - **order_id**, **product_id**, **seller_id**, **price**, **fee_value**: Order item details.
  - **Relationship**:
    - Belongs to one order.
    - Includes one product.
    - Sold by one seller (seller_conditions).

- **currency_exchange_rates**
  - **date**, **currency**: Composite primary key.
  - **exchange_rate_to_usd**: Exchange rate on a given date.
  - **Relationship**:
    - Provides exchange rates for currencies used by sellers.

```mermaid
erDiagram
    users {
        int id PK
        string name
        string email
        string address
    }
    seller_conditions {
        int id PK
        string seller_name
        float fee_percentage
        string currency
        int days_until_liquidation
    }
    products {
        int id PK
        string product_name
        int seller_id FK
        float price
    }
    orders {
        int id PK
        int user_id FK
        date order_date
        int installments
        float products_value
        float fee_value
        float interest_fee
        float total_transaction_value
    }
    order_items {
        int id PK
        int order_id FK
        int product_id FK
        int seller_id FK
        float price
        float fee_value
    }
    currency_exchange_rates {
        date date PK
        string currency PK
        float exchange_rate_to_usd
    }

    users ||--o{ orders : places
    orders ||--|{ order_items : contains
    products ||--o{ order_items : "is part of"
    products }o--|| seller_conditions : "offered by"
    seller_conditions ||--o{ order_items : "sells"
    seller_conditions ||--o{ products : "offers"
    currency_exchange_rates ||--o{ seller_conditions : "defines rates for"
```
