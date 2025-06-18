#!/usr/bin/env python3
"""
Fixed Merchant Pull Script with Updated Filtering:
- Communities: PERC_AUDIENCE >= 0.20
- Top 10 by COMPOSITE_INDEX
- Merchants: PERC_AUDIENCE >= 0.10 and AUDIENCE_COUNT > 100
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List
from snowflake_connection import query_to_dataframe

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    escaped = ["'" + c.replace("'", "''") + "'" for c in APPROVED_COMMUNITIES]
    return ', '.join(escaped)


class MerchantPull:
    def __init__(self):
        self.output_dir = Path("merchant_data")
        self.output_dir.mkdir(exist_ok=True)

        self.teams = [
            {
                'team_name': 'Utah Jazz',
                'team_name_short': 'Jazz',
                'league': 'NBA',
                'community_view': 'V_UTAH_JAZZ_SIL_COMMUNITY_INDEXING_ALL_TIME',
                'merchant_view': 'V_UTAH_JAZZ_SIL_COMMUNITY_MERCHANT_INDEXING_ALL_TIME',
                'audience_name': 'Utah Jazz Fans',
                'comparison_population': 'Local Gen Pop (Excl. Jazz)'
            },
            {
                'team_name': 'Dallas Cowboys',
                'team_name_short': 'Cowboys',
                'league': 'NFL',
                'community_view': 'V_DALLAS_COWBOYS_COMMUNITY_INDEXING_ALL_TIME',
                'merchant_view': 'V_DALLAS_COWBOYS_COMMUNITY_MERCHANT_INDEXING_ALL_TIME',
                'audience_name': 'Dallas Cowboys Fans',
                'comparison_population': 'Local Gen Pop (Excl. Dallas Cowboys Fans)'
            }
        ]

    def fetch_team_data(self, team_config: Dict[str, str]) -> List[Dict]:
        team_name = team_config['team_name']
        team_short = team_config['team_name_short']
        community_view = team_config['community_view']
        merchant_view = team_config['merchant_view']
        comparison_population = team_config['comparison_population']

        logger.info(f"\n🏀 Processing {team_name}...")

        approved_communities_sql = get_approved_communities_sql()

        communities_query = f"""
        SELECT 
            COMMUNITY,
            PERC_AUDIENCE,
            COMPOSITE_INDEX
        FROM 
            {community_view}
        WHERE 
            COMPARISON_POPULATION = '{comparison_population}'
            AND PERC_AUDIENCE >= 0.20
            AND COMMUNITY IN ({approved_communities_sql})
        ORDER BY COMPOSITE_INDEX DESC
        LIMIT 10
        """

        try:
            logger.info("Fetching top communities...")
            top_communities_df = query_to_dataframe(communities_query)
            # DEBUG: Show top merchant for each community and whether it qualifies
            logger.info("\n🔎 Debugging top merchants for each community:")

            for community in top_communities_df['COMMUNITY']:
                debug_query = f"""
                SELECT 
                    MERCHANT,
                    PERC_AUDIENCE,
                    AUDIENCE_COUNT
                FROM {merchant_view}
                WHERE COMMUNITY = '{community.replace("'", "''")}'
                  AND COMPARISON_POPULATION = '{comparison_population}'
                ORDER BY PERC_AUDIENCE DESC
                LIMIT 1
                """
                try:
                    merchant_df = query_to_dataframe(debug_query)
                    if merchant_df.empty:
                        logger.info(f"  - {community}: ❌ No merchants found")
                        continue

                    row = merchant_df.iloc[0]
                    qualifies = (
                            row['PERC_AUDIENCE'] >= 0.10 and row['AUDIENCE_COUNT'] > 100
                    )
                    status = "✅ QUALIFIES" if qualifies else "❌ FILTERED OUT"

                    logger.info(
                        f"  - {community}: {row['MERCHANT']} | {row['PERC_AUDIENCE']:.2%} | Count: {row['AUDIENCE_COUNT']} → {status}")

                except Exception as e:
                    logger.warning(f"  - {community}: ⚠️ Error fetching merchant: {e}")

            if top_communities_df.empty:
                logger.warning(f"No communities found for {team_name}")
                return []

            community_percentages = {row['COMMUNITY']: row['PERC_AUDIENCE'] for _, row in top_communities_df.iterrows()}
            communities = top_communities_df['COMMUNITY'].tolist()

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
                    ROW_NUMBER() OVER (
                        PARTITION BY tc.COMMUNITY 
                        ORDER BY m.PERC_AUDIENCE DESC
                    ) as rn
                FROM top_communities tc
                JOIN {merchant_view} m
                    ON tc.COMMUNITY = m.COMMUNITY
                WHERE m.COMPARISON_POPULATION = '{comparison_population}'
                    AND m.AUDIENCE_COUNT > 100
                    AND m.PERC_AUDIENCE >= 0.10
                    AND NOT (
                        tc.COMMUNITY = 'Live Entertainment Seekers' 
                        AND LOWER(m.SUBCATEGORY) LIKE '%professional sports%'
                    )
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

            logger.info("Fetching top merchants...")
            wheel_data_df = query_to_dataframe(merchants_query)

            if wheel_data_df.empty:
                logger.warning(f"No merchants found for {team_name}")
                return []

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

            return results

        except Exception as e:
            logger.error(f"Error for {team_name}: {e}")
            import traceback
            traceback.print_exc()
            return []

    def pull_all_merchants(self):
        all_results = []

        for team_config in self.teams:
            team_results = self.fetch_team_data(team_config)
            all_results.extend(team_results)

        if not all_results:
            logger.error("No merchant data found!")
            return None

        df = pd.DataFrame(all_results)
        df['Community_Perc_Display'] = df['Community_Perc_Audience'].apply(lambda x: f"{x:.2%}")
        df['Merchant_Perc_Display'] = df['Merchant_Perc_Audience'].apply(lambda x: f"{x:.2%}")

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_path = self.output_dir / f"merchant_pull_{timestamp}.csv"
        df.to_csv(csv_path, index=False)

        logger.info(f"\n✅ Saved merchant data to: {csv_path}")

        print("\n📊 Summary:")
        print(f"Total merchant-community pairs: {len(df)}")
        print("\nBy Team:")
        for team in df['Team'].unique():
            print(f"  - {team}: {len(df[df['Team'] == team])} pairs")
        print(f"\nUnique merchants: {df['Merchant'].nunique()}")
        print(f"Unique communities: {df['Community'].nunique()}")

        display_columns = ['Team', 'Community', 'Community_Perc_Display', 'Merchant',
                          'Merchant_Perc_Display', 'Merchant_Perc_Index', 'Verb']
        display_df = df[display_columns].head(10)
        display_df.columns = ['Team', 'Community', 'Comm %', 'Merchant', 'Merch %', 'Index', 'Verb']
        print("\nSample (first 10):")
        print(display_df.to_string(index=False))

        unique_merchants = sorted(df['Merchant'].unique())
        unique_path = self.output_dir / f"unique_merchants_{timestamp}.txt"
        with open(unique_path, 'w') as f:
            for merchant in unique_merchants:
                f.write(f"{merchant}\n")

        print(f"\n✅ Saved unique merchant list to: {unique_path}")
        return df


def main():
    print("🏆 Merchant Pull Script with Updated Filtering")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    puller = MerchantPull()
    try:
        puller.pull_all_merchants()
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
