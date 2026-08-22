import os
import json
from dotenv import load_dotenv
import snowflake.connector
from openai import OpenAI
from dotenv import load_dotenv

#loading environment variables from .env file
load_dotenv()

#openai api key
MODEL = "gpt-4o-mini"
sample_size = 5
client = OpenAI()

# giving the llm the context in the prompt
TOPICS = [
    "food quality",
    "delivery time",
    "customer service",
    "packaging",
    "app experience",
    "other"
]

SYSTEM_PROMPT = f"""
 You classify customer reviews for a food delivery app.

 For each review, that you will receive, please return:
 - sentiment_label: positive, negative, or neutral
 - sentiment_score: a number between -1.0 and 1.0, where 1.0 is the most positive and -1.0 is the most negative
 - topics: one of the {TOPICS}
 - key_issue: a short phrase of 6 words or less that describes the main issue in the review, if any. If there are no issues, return null

 Return the output in JSON format, with the following keys:
 {{
    "sentiment_label": "<sentiment_label>",
    "sentiment_score": <sentiment_score>,
    "topics": "<topics>",
    "key_issue": "<key_issue>"
 }}
"""

#connection with snowflake account
def get_connection():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    return conn

#creating the output table for the AI RESPONSE, in snowflake if it does not exist
def create_output_table(cursor):
    cursor.execute(" CREATE SCHEMA IF NOT EXISTS ZOMATO.AI")
    cursor.execute(""" CREATE TABLE IF NOT EXISTS ZOMATO.AI.REVIEW_ENRICHED (
        REVIEW_ID STRING,
        SENTIMENT_LABEL STRING,
        SENTIMENT_SCORE FLOAT,
        TOPICS STRING,
        KEY_ISSUE STRING,
        MODEL STRING,
        ENREICHED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
    ) 
    """)

# fetching the reviews from snowflake
def get_reviews_to_enrich(cursor):
    cursor.execute(f"""
        Select REVIEW_ID,
            COMMENT
        FROM ZOMATO.RAW.REVIEWS
        WHERE REVIEW_ID NOT IN (SELECT REVIEW_ID FROM ZOMATO.AI.REVIEW_ENRICHED)
        LIMIT {sample_size}
    """)

    return cursor.fetchall()

# now we will classify the reviews using the llm and enrich them with sentiment, topics and key issues
def classify_and_enrich_reviews(comment):

    #function give by openai 
    response = client.chat.completions.create(
        model = MODEL,
        temperature = 0,
        response_format = {"type": "json_object"},
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": comment}
        ]
    )

    answer = response.choices[0].message.content 

    return json.loads(answer)

#now once we have the answer from the llm, we will save it to the created table  review enreiched in the snowflake
def save_results(cursor, results):
    """
    In Python database programming, 
    %(key_name)s is a named placeholder (or bind variable).
    It acts as a labeled empty slot in your SQL string. 
    It tells the cursor: "Do not treat this text literally. Instead, look inside the incoming data dictionary, find the matching key name, and safely paste its value right here."""

    cursor.executemany(
        """
        INSERT INTO ZOMATO.AI.REVIEW_ENRICHED (REVIEW_ID, SENTIMENT_LABEL, SENTIMENT_SCORE, TOPICS, KEY_ISSUE, MODEL)
        VALUES(%(review_id)s, %(sentiment_label)s, %(sentiment_score)s, %(topics)s, %(key_issue)s, %(model)s)""", 
        results )
   

def main():
    conn = get_connection()
    cursor = conn.cursor()

    #1
    create_output_table(cursor)

    #2
    reviews = get_reviews_to_enrich(cursor)
    if len(reviews) == 0:
        print("No new reviews to enrich.")
        return
    
    #3
    print(f"enriching {len(reviews)} reviews . . . ")

    results = []
    for review_id, comment in reviews:
        try:
            json_labels = classify_and_enrich_reviews(comment)
            results.append(
                (
                    review_id,
                    json_labels["sentiment_label"],
                    json_labels["sentiment_score"],
                    json_labels["topics"],
                    json_labels["key_issue"],
                    MODEL
                )
            )
        except Exception as e:
            print(f"Error occured while processing review {review_id}: {e}")

    #4
    save_results(cursor, results)
    
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()








