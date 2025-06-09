#!/usr/bin/env python3
"""
Simple script to query Utah Jazz community indexing data from Snowflake
"""

import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os


def get_top_communities():
    """Connect to Snowflake and get top 10 communities by PERC_INDEX"""

    # Load environment variables
    load_dotenv()

    # Snowflake connection parameters
    config = {
        'account': 'JZJIKIA-GDA24737',
        'user': 'travis@twinbrain.ai',
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'warehouse': 'COMPUTE_WH',
        'database': 'SIL__TB_OTT_TEST',
        'schema': 'SC_TWINBRAINAI'
    }

    # Check if password exists
    if not config['password']:
        print("❌ Error: SNOWFLAKE_PASSWORD not found in .env file")
        print("Please create a .env file with: SNOWFLAKE_PASSWORD=your_password")
        return None

    try:
        # Connect to Snowflake
        print("🔄 Connecting to Snowflake...")
        conn = snowflake.connector.connect(**config)
        print("✅ Connected successfully!")

        # Create cursor
        cursor = conn.cursor()

        # Query for top 10 communities
        query = """
        SELECT 
            COMMUNITY,
            PERC_INDEX,
            PERC_AUDIENCE,
            PERC_AUDIENCE * 100 AS PERC_AUDIENCE_DISPLAY,
            AUDIENCE_COUNT,
            TOTAL_AUDIENCE_COUNT,
            RANK() OVER (ORDER BY PERC_INDEX DESC) AS RANK_POSITION
        FROM 
            V_UTAH_JAZZ_SIL_COMMUNITY_INDEXING_YOY
        WHERE 
            TRANSACTION_YEAR = '2025-01-01'
            AND COMPARISON_POPULATION = 'General Population'
        ORDER BY 
            PERC_INDEX DESC
        LIMIT 10
        """

        print("\n📊 Executing query...")
        cursor.execute(query)

        # Fetch results
        results = cursor.fetchall()

        # Get column names
        columns = [desc[0] for desc in cursor.description]

        # Convert to pandas DataFrame for easy display
        df = pd.DataFrame(results, columns=columns)

        # Print results
        print("\n🏀 TOP 10 COMMUNITIES FOR UTAH JAZZ FANS (2025)")
        print("=" * 80)

        for idx, row in df.iterrows():
            print(f"\n{row['RANK_POSITION']}. {row['COMMUNITY']}")
            print(f"   📈 Index: {row['PERC_INDEX']:.2f}%")
            print(f"   👥 Audience: {row['PERC_AUDIENCE_DISPLAY']:.2f}% ({row['AUDIENCE_COUNT']:,} fans)")

        print("\n" + "=" * 80)
        print("\n📋 Summary Table:")
        print(df[['RANK_POSITION', 'COMMUNITY', 'PERC_INDEX', 'PERC_AUDIENCE_DISPLAY']].to_string(index=False))

        # Close connection
        cursor.close()
        conn.close()
        print("\n✅ Connection closed")

        return df

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return None


if __name__ == "__main__":
    # Run the query
    data = get_top_communities()

    if data is not None:
        print("\n✅ Query completed successfully!")
        # You can access the DataFrame here for further processing
        # For example: data.to_csv('jazz_communities.csv', index=False)