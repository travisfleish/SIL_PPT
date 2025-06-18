#!/usr/bin/env python3
"""
Fixed Merchant Pull Script with Audience Percentages
Shows PERC_AUDIENCE for both communities and merchants
"""

import pandas as pd
import os
from pathlib import Path
import json
from datetime import datetime
import logging
from typing import Dict, List, Optional

# Import Snowflake connection
from snowflake_connection import query_to_dataframe

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# APPROVED COMMUNITIES (same as before)
APPROVED_COMMUNITIES = {
    "Adult Recreational Sports": "Plays",
    "Alternative Wellness": "Shops",
    "Beauty Enthusiasts": "Buys",
    "Big Sporting Events": "Attends",
    "Bookworms": "Reads",
    "Boutique Fitness Enthusiasts": "Member of",
    "Budget Travelers": "Travels",
    "Casual Outdoor Enthusiasts": "Shops",
    "Charitable Givers": "Donates",
    "Collectors": "Collects",
    "College Sports": "Fan of",
    "Concerts and Festivals": "Attends",
    "Cultural Arts": "Enjoys",
    "Daters": "Dates on",
    "Disney Diehards": "Visits",
    "DIY Arts & Crafts": "Creates",
    "Dollar Store Shoppers": "Saves with",
    "Domestic Decorators": "Decorates with",
    "Drinkers": "Drinks",
    "Eco Conscious": "Shops",
    "Emerging Sports Fan": "Fan of",
    "Endurance Athletes": "Joins",
    "Fans of Womens Sports (FOWS)": "Fan of",
    "Fishing Fanatics": "Spends on",
    "Fitness Enthusiasts": "Joins",
    "Gambler": "Bets on",
    "Gamers": "Games on",
    "Golfers": "Shops",
    "Hardcore Outdoor Enthusiasts": "Buys",
    "Healthy Eaters": "Eats",
    "Health Nut": "Shops",
    "Live Entertainment Seekers": "Attends",
    "Luxury Brand Shoppers": "Splurges on",
    "Luxury Fitness Clubs": "Joins",
    "Mindful": "Focuses with",
    "Motorcycle Enthusiasts": "Rides with",
    "Movie Buffs": "Buys",
    "Olympics Fans": "Fan of",
    "Outdoor Enthusiasts": "Buys",
    "Pet Owners": "Buys",
    "Pickleball": "Plays",
    "Runners": "Runs with",
    "Skate": "Skates with",
    "Skiers": "Skies with",
    "Sneakerheads": "Buys",
    "Sober Curious": "Drinks",
    "Sportstainment": "Plays at",
    "Sports Bettor": "Bets with",
    "Sports Merchandise Shopper": "Shops",
    "Sports Streamer": "Streams",
    "Surf": "Surfs",
    "Tech Savvy": "Buys",
    "Theme Parkers": "Visits",
    "Traditional Gyms": "Joins",
    "Travelers": "Travels with",
    "Trend Setters": "Buys",
    "Values Driven": "Shops",
    "Wellness Warriors": "Buys",
    "Yogis": "Stretches with",
    "Youth Sports": "Plays",
}


def get_approved_communities_sql():
    """Generate SQL IN clause for approved communities"""
    if not APPROVED_COMMUNITIES:
        return "''"

    # Escape single quotes in community names
    escaped = []
    for c in APPROVED_COMMUNITIES.keys():
        escaped_name = c.replace("'", "''")
        escaped.append(f"'{escaped_name}'")
    return ', '.join(escaped)


class MerchantPull:
    """Pull merchants for teams using same logic as wheel generator"""

    def __init__(self):
        # Create output directory
        self.output_dir = Path("merchant_data")
        self.output_dir.mkdir(exist_ok=True)

        # Team configurations with CORRECTED comparison populations
        self.teams = [
            {
                'team_name': 'Utah Jazz',
                'team_name_short': 'Jazz',
                'league': 'NBA',
                'community_view': 'V_UTAH_JAZZ_SIL_COMMUNITY_INDEXING_ALL_TIME',
                'merchant_view': 'V_SIL_COMMUNITY_MERCHANT_INDEXING_ALL_TIME',
                'audience_name': 'Utah Jazz Fans',
                'comparison_population': 'Local Gen Pop (Excl. Jazz)'  # Keep as is - working
            },
            {
                'team_name': 'Dallas Cowboys',
                'team_name_short': 'Cowboys',
                'league': 'NFL',
                'community_view': 'V_DALLAS_COWBOYS_COMMUNITY_INDEXING_ALL_TIME',
                'merchant_view': 'V_DALLAS_COWBOYS_COMMUNITY_MERCHANT_INDEXING_ALL_TIME',
                'audience_name': 'Dallas Cowboys Fans',
                'comparison_population': 'Local Gen Pop (Excl. Dallas Cowboys Fans)'  # FIXED!
            }
        ]

    def fetch_team_data(self, team_config: Dict[str, str]) -> List[Dict]:
        """Fetch top 10 communities and their top merchant for a team"""

        team_name = team_config['team_name']
        team_short = team_config['team_name_short']
        community_view = team_config['community_view']
        merchant_view = team_config['merchant_view']
        comparison_population = team_config['comparison_population']

        logger.info(f"\n🏀 Processing {team_name}...")

        # Get approved communities SQL
        approved_communities_sql = get_approved_communities_sql()

        # Community query with corrected comparison population
        communities_query = f"""
        SELECT 
            COMMUNITY,
            PERC_AUDIENCE,
            COMPOSITE_INDEX
        FROM 
            {community_view}
        WHERE 
            COMPARISON_POPULATION = '{comparison_population}'
            AND PERC_AUDIENCE >= 0.25
            AND COMMUNITY IN ({approved_communities_sql})
        ORDER BY COMPOSITE_INDEX DESC
        LIMIT 10
        """

        try:
            logger.info(f"Fetching approved communities from Snowflake...")
            logger.info(f"Using comparison population: '{comparison_population}'")
            logger.info(f"Total approved communities: {len(APPROVED_COMMUNITIES)}")

            top_communities_df = query_to_dataframe(communities_query)

            if top_communities_df.empty:
                logger.warning(f"No approved communities found for {team_name}")

                # Debug: Show what communities exist for other populations
                debug_query = f"""
                SELECT 
                    COMPARISON_POPULATION,
                    COUNT(DISTINCT COMMUNITY) as community_count
                FROM {community_view}
                WHERE COMMUNITY IN ({approved_communities_sql})
                GROUP BY COMPARISON_POPULATION
                """

                try:
                    debug_df = query_to_dataframe(debug_query)
                    if not debug_df.empty:
                        logger.info("Available comparison populations with approved communities:")
                        for _, row in debug_df.iterrows():
                            logger.info(f"  - {row['COMPARISON_POPULATION']}: {row['COMMUNITY_COUNT']} communities")
                except:
                    pass

                return []

            # Create a dictionary to store community percentages
            community_percentages = {}
            for _, row in top_communities_df.iterrows():
                community_percentages[row['COMMUNITY']] = row['PERC_AUDIENCE']

            communities = top_communities_df['COMMUNITY'].tolist()
            logger.info(f"Found {len(communities)} top communities")

            # Print communities for debugging
            for i, (_, row) in enumerate(top_communities_df.iterrows(), 1):
                logger.info(f"   {i}. {row['COMMUNITY']} (Index: {row['COMPOSITE_INDEX']:.0f}, Audience: {row['PERC_AUDIENCE']:.2%})")

            # Enhanced merchant query that includes PERC_AUDIENCE
            # IMPORTANT: Now ranking merchants by PERC_AUDIENCE instead of PERC_INDEX
            # EXCLUDES professional sports subcategory for Live Entertainment Seekers
            merchants_query = f"""
            WITH top_communities AS (
                {communities_query}
            ),
            ranked_merchants AS (
                SELECT 
                    tc.COMMUNITY,
                    tc.PERC_AUDIENCE as COMMUNITY_PERC_AUDIENCE,
                    tc.COMPOSITE_INDEX as COMMUNITY_INDEX,
                    m.MERCHANT,
                    m.CATEGORY,
                    m.SUBCATEGORY,
                    m.PERC_INDEX,
                    m.PERC_AUDIENCE as MERCHANT_PERC_AUDIENCE,
                    m.AUDIENCE_TOTAL_SPEND,
                    m.AUDIENCE_COUNT,
                    ROW_NUMBER() OVER (PARTITION BY tc.COMMUNITY ORDER BY m.PERC_AUDIENCE DESC) as rn
                FROM top_communities tc
                JOIN {merchant_view} m
                    ON tc.COMMUNITY = m.COMMUNITY
                WHERE m.COMPARISON_POPULATION = '{comparison_population}'
                    AND m.AUDIENCE_COUNT > 10
                    -- Exclude professional sports subcategory for Live Entertainment Seekers
                    AND NOT (tc.COMMUNITY = 'Live Entertainment Seekers' 
                            AND LOWER(m.SUBCATEGORY) LIKE '%professional sports%')
            )
            SELECT 
                COMMUNITY,
                COMMUNITY_PERC_AUDIENCE,
                COMMUNITY_INDEX,
                MERCHANT,
                CATEGORY,
                SUBCATEGORY,
                PERC_INDEX,
                MERCHANT_PERC_AUDIENCE
            FROM ranked_merchants 
            WHERE rn = 1
            ORDER BY MERCHANT_PERC_AUDIENCE DESC
            """

            logger.info("Fetching top merchants for each community...")
            wheel_data_df = query_to_dataframe(merchants_query)

            if wheel_data_df.empty:
                logger.warning(f"No merchants found for {team_name}")
                return []

            # Convert to list of dicts with both percentages
            results = []
            for _, row in wheel_data_df.iterrows():
                results.append({
                    'Team': team_name,
                    'League': team_config['league'],
                    'Community': row['COMMUNITY'],
                    'Community_Perc_Audience': row['COMMUNITY_PERC_AUDIENCE'],
                    'Community_Index': row['COMMUNITY_INDEX'],
                    'Merchant': row['MERCHANT'],
                    'Category': row.get('CATEGORY', ''),
                    'Subcategory': row.get('SUBCATEGORY', ''),
                    'Merchant_Perc_Index': row['PERC_INDEX'],
                    'Merchant_Perc_Audience': row['MERCHANT_PERC_AUDIENCE'],
                    'Verb': APPROVED_COMMUNITIES.get(row['COMMUNITY'], 'Shops at')
                })

            logger.info(f"✅ Found {len(results)} merchant-community pairs for {team_name}")

            return results

        except Exception as e:
            logger.error(f"Error fetching data for {team_name}: {e}")
            import traceback
            traceback.print_exc()
            return []

    def pull_all_merchants(self):
        """Pull merchants for all teams and save to CSV"""

        all_results = []

        for team_config in self.teams:
            team_results = self.fetch_team_data(team_config)
            all_results.extend(team_results)

        if not all_results:
            logger.error("No merchant data found for any team!")
            return None

        # Convert to DataFrame
        df = pd.DataFrame(all_results)

        # Format percentage columns for display
        df['Community_Perc_Display'] = df['Community_Perc_Audience'].apply(lambda x: f"{x:.2%}")
        df['Merchant_Perc_Display'] = df['Merchant_Perc_Audience'].apply(lambda x: f"{x:.2%}")

        # Save to CSV with raw values
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_path = self.output_dir / f"merchant_pull_{timestamp}.csv"
        df.to_csv(csv_path, index=False)

        logger.info(f"\n✅ Saved merchant data to: {csv_path}")

        # Print summary
        print("\n📊 Summary:")
        print(f"Total merchant-community pairs: {len(df)}")
        print(f"\nBy Team:")
        for team in df['Team'].unique():
            team_count = len(df[df['Team'] == team])
            print(f"  - {team}: {team_count} pairs")

        print(f"\nUnique merchants: {df['Merchant'].nunique()}")
        print(f"Unique communities: {df['Community'].nunique()}")

        # Show sample data with formatted percentages
        print("\nSample data (first 10 rows):")
        display_columns = ['Team', 'Community', 'Community_Perc_Display', 'Merchant',
                          'Merchant_Perc_Display', 'Merchant_Perc_Index', 'Verb']
        display_df = df[display_columns].head(10)
        display_df.columns = ['Team', 'Community', 'Comm %', 'Merchant', 'Merch %', 'Index', 'Verb']
        print(display_df.to_string(index=False))

        # Also save a unique merchants list
        unique_merchants = sorted(df['Merchant'].unique())
        unique_path = self.output_dir / f"unique_merchants_{timestamp}.txt"
        with open(unique_path, 'w') as f:
            for merchant in unique_merchants:
                f.write(f"{merchant}\n")

        print(f"\n✅ Also saved unique merchant list to: {unique_path}")

        return df


def main():
    """Main execution function"""

    print("🏆 Merchant Pull Script with Audience Percentages")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Initialize and run
    puller = MerchantPull()

    try:
        df = puller.pull_all_merchants()

    except Exception as e:
        logger.error(f"Error in main process: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()