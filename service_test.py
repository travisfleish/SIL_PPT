#!/usr/bin/env python3
"""
Snowflake connection for MVP - handles MFA via phone
"""

import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os


class SnowflakeMVPConnection:
    """Simple Snowflake connection that handles MFA via phone"""

    def __init__(self):
        load_dotenv()

        self.config = {
            'account': 'JZJIKIA-GDA24737',
            'user': 'travis@twinbrain.ai',
            'password': os.getenv('SNOWFLAKE_PASSWORD'),  # Your regular password
            'warehouse': 'COMPUTE_WH',
            'database': 'SIL__TB_OTT_TEST',
            'schema': 'SC_TWINBRAINAI'
        }
        self._connection = None

    def get_connection(self):
        """Get connection - reuses existing if available"""
        if self._connection is None or self._connection.is_closed():
            print("📱 Connecting to Snowflake...")
            print("   Check your phone for Duo push notification!")
            self._connection = snowflake.connector.connect(**self.config)
            print("✅ Connected successfully!")
        return self._connection

    def query_to_dataframe(self, query):
        """Execute query and return pandas DataFrame"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        df = pd.DataFrame.from_records(
            iter(cursor),
            columns=[desc[0] for desc in cursor.description]
        )
        return df

    def close(self):
        """Close connection when done"""
        if self._connection:
            self._connection.close()
            print("👋 Connection closed")


# Initialize once at the start of your app
db = SnowflakeMVPConnection()


# Example functions for your Sports App
def get_utah_jazz_fans():
    """Get Jazz fan data - no auth needed after first connection"""
    return db.query_to_dataframe("""
        SELECT * FROM V_UTAH_JAZZ_SIL_MERCHANT_INDEXING_YOY
        LIMIT 1000
    """)


def get_community_indexing(team_name='Utah Jazz'):
    """Get community indexing data"""
    return db.query_to_dataframe(f"""
        SELECT * FROM V_COMMUNITY_INDEXING_YOY
        WHERE team_name = '{team_name}'
    """)


def get_category_insights(team_name='Utah Jazz'):
    """Get category insights for PowerPoint generation"""
    return db.query_to_dataframe(f"""
        SELECT * FROM V_UTAH_JAZZ_SIL_CATEGORY_INDEXING_YOY
        LIMIT 20
    """)


# Test it
if __name__ == "__main__":
    print("Testing Snowflake connection...")

    # First connection will trigger phone MFA
    fans = get_utah_jazz_fans()
    print(f"✅ Retrieved {len(fans)} fan records")
    print("\nSample data (first 10 rows):")
    print(fans.head(10))
    print(f"\nColumns: {list(fans.columns)}")

    # Subsequent queries use same connection (no more MFA)
    categories = get_category_insights()
    print(f"\n✅ Retrieved {len(categories)} category insights")
    print("\nSample category data (first 10 rows):")
    print(categories.head(10))

    # Close when done
    db.close()