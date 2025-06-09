#!/usr/bin/env python3
"""
Snowflake connection using Programmatic Access Token (PAT)
No MFA prompts required!
"""

import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os


class SnowflakeTokenConnection:
    """Connect to Snowflake using access token - bypasses MFA"""

    def __init__(self):
        load_dotenv()

        # Your token
        self.token = os.getenv('SNOWFLAKE_TOKEN',
                               'eyJraWQiOiIzMDgwNTU3OTkxNjQzOTU1OCIsImFsZyI6IkVTMjU2In0.eyJwIjoiMTgzNjE1NTY1MjoxODM2MTU1NzgwIiwiaXNzIjoiU0Y6MjAxNyIsImV4cCI6MTc1MjA2OTY1OH0.udoK176mvt93UbC7sVNd9jXvxkhUbOOdpF3gmY3VMYwydlpnohnKUTjiDDZowgSCZGFO3mQjszVVHwJF5pAoCA'
                               )

        self.config = {
            'account': 'JZJIKIA-GDA24737',
            'user': 'travis@twinbrain.ai',
            'authenticator': 'oauth',  # This tells Snowflake to use token auth
            'token': self.token,  # The token replaces password
            'warehouse': 'COMPUTE_WH',
            'database': 'SIL__TB_OTT_TEST',
            'schema': 'SC_TWINBRAINAI'
        }

    def get_connection(self):
        """Get connection using token - no MFA prompt!"""
        try:
            print("🔄 Connecting with access token...")
            conn = snowflake.connector.connect(**self.config)
            print("✅ Connected successfully (no MFA needed!)")
            return conn
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            # Try alternative method if oauth fails
            print("\n🔄 Trying alternative token authentication...")
            self.config['authenticator'] = 'snowflake'
            self.config['password'] = self.token  # Some versions use token as password
            del self.config['token']

            try:
                conn = snowflake.connector.connect(**self.config)
                print("✅ Connected with alternative method!")
                return conn
            except Exception as e2:
                print(f"❌ Alternative method also failed: {e2}")
                raise

    def query_to_dataframe(self, query):
        """Execute query and return pandas DataFrame"""
        conn = self.get_connection()
        df = pd.read_sql(query, conn)
        conn.close()
        return df


# Alternative: Try key pair authentication if token doesn't work
class SnowflakeSimpleConnection:
    """Fallback to password + handle MFA via browser"""

    def __init__(self):
        load_dotenv()

        self.config = {
            'account': 'JZJIKIA-GDA24737',
            'user': 'travis@twinbrain.ai',
            'password': os.getenv('SNOWFLAKE_PASSWORD'),  # Your regular password
            'authenticator': 'externalbrowser',  # This handles MFA via browser
            'warehouse': 'COMPUTE_WH',
            'database': 'SIL__TB_OTT_TEST',
            'schema': 'SC_TWINBRAINAI'
        }

    def get_connection(self):
        print("🔄 Opening browser for authentication...")
        print("📱 Check your browser to complete login (handles MFA automatically)")
        conn = snowflake.connector.connect(**self.config)
        print("✅ Connected successfully!")
        return conn


# Test both methods
if __name__ == "__main__":
    # First try token auth
    try:
        print("Attempting token authentication...")
        db = SnowflakeTokenConnection()
        conn = db.get_connection()

        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_TIMESTAMP()")
        user, timestamp = cursor.fetchone()
        print(f"✅ Authenticated as: {user}")
        conn.close()

    except Exception as e:
        print(f"\n❌ Token auth failed: {e}")
        print("\n" + "=" * 60)
        print("Falling back to browser authentication...")
        print("=" * 60 + "\n")

        # Fallback to browser auth
        db = SnowflakeSimpleConnection()
        conn = db.get_connection()

        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_TIMESTAMP()")
        user, timestamp = cursor.fetchone()
        print(f"✅ Authenticated as: {user}")
        conn.close()