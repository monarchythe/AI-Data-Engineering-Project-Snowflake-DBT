import os
import numpy as np
import pandas as pd 
import streamlit as st
import snowflake.connector
from openai import OpenAI
from dotenv import load_dotenv
import json

load_dotenv()

MODEL = "gpt-4o-mini"

FORBIDDEN_WORDS = ['drop', 
                   'delete', 
                   'truncate',
                   'alter',
                   'update','insert','create','replace','grant', 'revoke']

EXAMPLE_QUESTIONS = [
    "top 10 cities by GMV?",
    "which cuisin has the most orders?",
    "average delivery time by city, worst first?",
    "cancel rate by payment method"
]
client = OpenAI(api_key=os.getenv("OPEN_API_KEY"))

#geting the schema , so that the LLM model is aware of the tables and columns
SCHEMA = """
Tables available (Snowflake). Use bare table names, no database or schema prefix.
 
FCT_ORDERS(order_id, order_date, customer_id, restaurant_id, city, cuisine,
           payment_method, order_status, is_delivered, sales_amount, discount,
           delivery_fee, gst, customer_rating, delivery_time_min)
DIM_RESTAURANT(restaurant_id, restaurant_name, city, cuisine, rating, cost_for_two)
DIM_CUSTOMER(customer_id, customer_name, age, age_segment, gender, city)
MART_DAILY_CITY_REVENUE(order_date, city, orders, cancel_rate, gmv, aov)
MART_RESTAURANT_PERFORMANCE(restaurant_id, restaurant_name, city, cuisine,
                            orders, revenue, avg_customer_rating, cancel_rate)
MART_DELIVERY_SLA(city, order_hour, delivered_orders, p50_delivery_min, late_rate)

 
Note: gmv means delivered revenue. Prefer the MART_ tables when they fit the question.
"""

SYSTEM_PROMPT = f"""
You are a Snowflake SQL expert. Wrtie One SELECT query that answers the question.

Rules:
- SELECT queries only, never modify data.
- Use bare table names (like FCT_ORDERS, not ZOMATO.MARTS.FCT_ORDERS) for writing the SELECT query
- Add a LIMIT of 100 or less, unless the question asks for aggregates like single total.
- Reply in JSON in this exact format: {{"sql": "your query here"}}

{SCHEMA}
"""
#connection with snowflake account
def get_connection():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema= "MARTS",
        role = "DBT_ROLE"
    )
    return conn

#one function to handle the question ffrom the user 
def generate_sql(question):
    response = client.chat.completions.create(
        model = MODEL,
        temperature= 0,
        response_format={"type":"json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question}
        ]
    )
    answer = response.choices[0].message.content
    sql = json.loads(answer)["sql"]

    sql = sql.replace("ZOMATO.MARTS.", "").replace("ZOMATO.","")

    return sql.strip().rstrip("")

#checkingh the sql query 
def is_sql_safe(sql):
    lowered = sql.lower()

    if not lowered.startswith("select") and not lowered.startswith("with"):
        return False # tf?

    for word in FORBIDDEN_WORDS:
        if word in lowered:
            return False

    return True

#running the query
def run_the_query(sql):
    conn = get_connection()
    cursor = conn.cursor()
    return cursor.execute(sql).fetch_pandas_all()
    
st.markdown(
    """
    <div style="display: flex; align-items: center; gap: 15px; margin-bottom: 10px;">
        <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSOzjFbmdHrz9RI3UdKk4pHspka9bvnXV-YCaN9Cmb5rA&s=10" width="50" style="object-fit: contain;">
        <h1 style="margin: 0; padding: 0; font-weight: 700; font-size: 2.5rem;">Chat with your Zomato Data</h1>
    </div>
    """,
    unsafe_allow_html=True
)
st.caption(f"Ask in English, {MODEL} will write the SQL command, SNOWFLAKE will execute it")


with st.sidebar:
    st.header("Example Questions")
    for que in EXAMPLE_QUESTIONS:
        st.markdown(f" - {que}")

question = st.text_input("Enter your question here", 
                         placeholder= "e.g Top 10 restaurants by revenue in Mumbai")


if question:
    sql = generate_sql(question)
    st.code(sql, language="sql")

    if not is_sql_safe(sql):
        st.error("The generated SQL is not safe to run. Please modify your question.")
    else:
        try:
           df =  run_the_query(sql)
           st.success(f"{len(df)} rows returned from Snowflake Delta Lake")
           st.dataframe(df, hide_index=True)

           #visualise
           if len(df.columns) == 2 and pd.api.types.is_numeric_dtype(df.iloc[:,1]):
               st.bar_chart(df, x=df.columns[0], y=df.columns[1])
               
        except Exception as e:
            st.error(f"Error encountered while runing the SQL query: {e}")


