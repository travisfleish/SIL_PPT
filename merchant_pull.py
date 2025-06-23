#!/usr/bin/env python3
"""
Fixed Merchant Pull Script with Threshold Status:
- Communities: PERC_AUDIENCE >= 0.20, Top 10 by COMPOSITE_INDEX
- Merchants: Top 1 per community (unchanged logic)
- Added: Threshold qualification status in CSV
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

# Threshold constants for qualification check
AUDIENCE_COUNT_THRESHOLD = 100
PERC_AUDIENCE_THRESHOLD = 0.10

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


def check_qualification(audience_count, perc_audience):
    """Check if a merchant meets the threshold criteria (only count and percentage)"""

    # Only check the two main thresholds
    meets_count = audience_count > AUDIENCE_COUNT_THRESHOLD
    meets_perc = perc_audience >= PERC_AUDIENCE_THRESHOLD

    if not meets_count:
        return False, "LOW_AUDIENCE_COUNT"
    elif not meets_perc:
        return False, "LOW_PERC_AUDIENCE"
    else:
        return True, "MEETS_CRITERIA"


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

            # Enhanced debugging with qualification check
            logger.info("\n🔎 Debugging top merchants for each community:")

            for community in top_communities_df['COMMUNITY']:
                debug_query = f"""
                SELECT 
                    MERCHANT,
                    PERC_AUDIENCE,
                    AUDIENCE_COUNT,
                    SUBCATEGORY
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
                    qualifies, reason = check_qualification(
                        row['AUDIENCE_COUNT'],
                        row['PERC_AUDIENCE']
                    )
                    status = "✅ QUALIFIES" if qualifies else "❌ FILTERED OUT"

                    logger.info(
                        f"  - {community}: {row['MERCHANT']} | {row['PERC_AUDIENCE']:.2%} | "
                        f"Count: {row['AUDIENCE_COUNT']} | Reason: {reason} → {status}"
                    )

                except Exception as e:
                    logger.warning(f"  - {community}: ⚠️ Error fetching merchant: {e}")

            if top_communities_df.empty:
                logger.warning(f"No communities found for {team_name}")
                return []

            communities = top_communities_df['COMMUNITY'].tolist()

            # Merchants query with professional sports exclusion built-in
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
                MERCHANT_PERC_AUDIENCE,
                AUDIENCE_COUNT
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
            qualifying_count = 0

            for _, row in wheel_data_df.iterrows():
                # Check qualification using our function (only count and percentage)
                qualifies, reason = check_qualification(
                    row['AUDIENCE_COUNT'],
                    row['MERCHANT_PERC_AUDIENCE']
                )

                if qualifies:
                    qualifying_count += 1

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
                    'Audience_Count': row['AUDIENCE_COUNT'],
                    'Verb': APPROVED_COMMUNITIES.get(row['COMMUNITY'], 'Shops at'),
                    # New threshold columns
                    'Meets_Threshold': qualifies,
                    'Threshold_Status': 'QUALIFIES' if qualifies else 'FILTERED_OUT',
                    'Filter_Reason': reason,
                    'Audience_Count_Threshold': AUDIENCE_COUNT_THRESHOLD,
                    'Perc_Audience_Threshold': PERC_AUDIENCE_THRESHOLD
                })

            logger.info(f"📊 {team_name} Summary: {qualifying_count}/{len(results)} qualify")
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
        csv_path = self.output_dir / f"merchant_pull_with_thresholds_{timestamp}.csv"
        df.to_csv(csv_path, index=False)

        logger.info(f"\n✅ Saved merchant data to: {csv_path}")

        # Enhanced summary
        total_merchants = len(df)
        qualifying_merchants = len(df[df['Meets_Threshold']])

        print("\n📊 Summary:")
        print(f"Total merchant-community pairs: {total_merchants}")
        print(
            f"Merchants meeting threshold: {qualifying_merchants} ({qualifying_merchants / total_merchants * 100:.1f}%)")
        print(
            f"Merchants filtered out: {total_merchants - qualifying_merchants} ({(total_merchants - qualifying_merchants) / total_merchants * 100:.1f}%)")

        print("\nBy Team:")
        for team in df['Team'].unique():
            team_df = df[df['Team'] == team]
            team_qualifying = len(team_df[team_df['Meets_Threshold']])
            print(f"  - {team}: {team_qualifying}/{len(team_df)} qualify")

        print("\nBy Filter Reason:")
        filter_counts = df['Filter_Reason'].value_counts()
        for reason, count in filter_counts.items():
            print(f"  - {reason}: {count}")

        display_columns = ['Team', 'Community', 'Community_Perc_Display', 'Merchant',
                           'Merchant_Perc_Display', 'Audience_Count', 'Threshold_Status', 'Filter_Reason', 'Verb']
        display_df = df[display_columns].head(20)  # Show all results
        display_df.columns = ['Team', 'Community', 'Comm %', 'Merchant', 'Merch %', 'Count', 'Status', 'Reason', 'Verb']
        print("\nAll Results:")
        print(display_df.to_string(index=False))

        # Save qualifying merchants list for backwards compatibility
        qualifying_merchants = df[df['Meets_Threshold']]['Merchant'].unique()
        unique_path = self.output_dir / f"unique_merchants_{timestamp}.txt"
        with open(unique_path, 'w') as f:
            for merchant in sorted(qualifying_merchants):
                f.write(f"{merchant}\n")

        print(f"\n✅ Saved qualifying merchant list to: {unique_path}")
        return df


def main():
    print("🏆 Merchant Pull Script with Threshold Status")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Thresholds: Audience Count > {AUDIENCE_COUNT_THRESHOLD}, Perc Audience >= {PERC_AUDIENCE_THRESHOLD:.1%}")
    print()

    puller = MerchantPull()
    try:
        puller.pull_all_merchants()
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()