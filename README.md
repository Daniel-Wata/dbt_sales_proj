
# DBT Sales Project

## Idea
Use fake data to work with dbt models in conjunction with a bigquery data warehouse, the purpose is to have raw, bronze, silver and gold layers and process the data among the layers using dbt.

## The data
I designed a relational database of sales with some basic tables:

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

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
