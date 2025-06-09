#!/usr/bin/env python3
"""
Complete script to query Snowflake and generate audience index chart
Refactored to use centralized connection manager
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Rectangle
import numpy as np
from datetime import datetime

# Import centralized connection manager
from snowflake_connection import query_to_dataframe


def query_snowflake():
    """Get top 10 communities by COMPOSITE_INDEX using centralized connection"""

    # Define the exclusion list as a constant for better maintainability
    EXCLUDED_SPORTS = [
        'General Sports Fans', 'Fans of Men\'s Sports (FOMS)', 'Fan\'s of Men\'s Sports (FOMS)',
        'NBA', 'Basketball', 'NFL', 'Football', 'American Football', 'College Football',
        'NHL', 'Hockey', 'Ice Hockey', 'MLB', 'Baseball', 'MLS', 'Soccer', 'Football (Soccer)',
        'Premier League', 'La Liga', 'Bundesliga', 'Serie A', 'Ligue 1', 'Champions League',
        'PGA', 'Golf', 'NASCAR', 'Formula 1', 'F1', 'Auto Racing', 'Boxing', 'MMA', 'UFC',
        'Wrestling', 'WWE'
    ]

    # Build the NOT IN clause
    exclusion_list = ', '.join([f"'{sport}'" for sport in EXCLUDED_SPORTS])

    query = f"""
    SELECT 
        COMMUNITY,
        PERC_INDEX,
        COMPOSITE_INDEX,
        PERC_AUDIENCE * 100 AS PERC_AUDIENCE_DISPLAY,
        AUDIENCE_COUNT,
        TOTAL_AUDIENCE_COUNT
    FROM 
        V_UTAH_JAZZ_SIL_COMMUNITY_INDEXING_ALL_TIME
    WHERE 
        COMPARISON_POPULATION = %(comparison_pop)s
        AND PERC_AUDIENCE >= %(min_audience)s
        AND COMMUNITY NOT IN ({exclusion_list})
        AND UPPER(COMMUNITY) NOT LIKE '%NBA%'
        AND UPPER(COMMUNITY) NOT LIKE '%NFL%'
        AND UPPER(COMMUNITY) NOT LIKE '%NHL%'
        AND UPPER(COMMUNITY) NOT LIKE '%MLB%'
        AND UPPER(COMMUNITY) NOT LIKE '%MLS%'
        AND UPPER(COMMUNITY) NOT LIKE '%FOOTBALL%'
        AND UPPER(COMMUNITY) NOT LIKE '%BASKETBALL%'
        AND UPPER(COMMUNITY) NOT LIKE '%HOCKEY%'
        AND UPPER(COMMUNITY) NOT LIKE '%BASEBALL%'
        AND UPPER(COMMUNITY) NOT LIKE '%SOCCER%'
        AND UPPER(COMMUNITY) NOT LIKE '%GOLF%'
        AND UPPER(COMMUNITY) NOT LIKE '%NASCAR%'
        AND UPPER(COMMUNITY) NOT LIKE '%FORMULA%'
        AND UPPER(COMMUNITY) NOT LIKE '%BOXING%'
        AND UPPER(COMMUNITY) NOT LIKE '%UFC%'
        AND UPPER(COMMUNITY) NOT LIKE '%MMA%'
        AND UPPER(COMMUNITY) NOT LIKE '%WRESTLING%'
    ORDER BY 
        COMPOSITE_INDEX DESC
    LIMIT %(limit)s
    """

    # Parameters for the query
    params = {
        'comparison_pop': 'Local Gen Pop (Excl. Jazz)',
        'min_audience': 0.15,
        'limit': 10
    }

    try:
        print("📊 Executing query...")
        print(f"   - Comparison Population: {params['comparison_pop']}")
        print(f"   - Min Audience: {params['min_audience'] * 100:.0f}%")
        print(f"   - Excluding {len(EXCLUDED_SPORTS)} sports communities")

        df = query_to_dataframe(query, params)

        print(f"✅ Data retrieved successfully! Got {len(df)} communities")
        return df

    except Exception as e:
        print(f"❌ Error querying Snowflake: {str(e)}")
        return None


def create_audience_index_chart(df, save_path='audience_index_chart.png'):
    """
    Create the audience index chart matching the SIL style.
    No changes needed here - visualization logic remains the same.
    """
    print("\n🎨 Creating chart...")

    # Sort by PERC_AUDIENCE_DISPLAY ascending for chart (highest at top)
    df_sorted = df.sort_values('PERC_AUDIENCE_DISPLAY', ascending=True)

    communities = df_sorted['COMMUNITY'].tolist()
    perc_indices = df_sorted['PERC_INDEX'].tolist()
    perc_audiences = df_sorted['PERC_AUDIENCE_DISPLAY'].tolist()

    # Create figure
    fig, ax = plt.subplots(figsize=(10, 8), facecolor='white')
    y_positions = np.arange(len(communities))
    bar_height = 0.7

    # Colors
    bar_color = '#5B9BD5'
    background_color = '#E0E0E0'
    label_color = '#FFD966'
    line_color = '#404040'

    # Scale setup
    max_index = max(perc_indices)
    max_scale = int(np.ceil(max_index / 100) * 100)
    bar_scale_factor = (max_scale * 0.8) / 100

    # Draw background bars
    for i, y in enumerate(y_positions):
        ax.add_patch(Rectangle((0, y - bar_height / 2), max_scale, bar_height,
                               facecolor=background_color, zorder=1))

    # Draw audience percentage bars
    for i, (y, perc_aud) in enumerate(zip(y_positions, perc_audiences)):
        bar_length = perc_aud * bar_scale_factor
        ax.add_patch(Rectangle((0, y - bar_height / 2), bar_length, bar_height,
                               facecolor=bar_color, zorder=2))

        # Add percentage label
        ax.text(bar_length + 10, y, f"{perc_aud:.0f}%",
                va='center', ha='left', fontsize=11, fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.3", facecolor=label_color, edgecolor='none'),
                color='black', zorder=4)

    # Draw index lines
    for i, perc_idx in enumerate(perc_indices):
        ax.plot([0, perc_idx], [y_positions[i], y_positions[i]],
                color=line_color, linewidth=2.5, zorder=3)

    # Configure axes
    ax.set_xlim(0, max_scale)
    ax.set_ylim(-0.5, len(communities) - 0.5)
    ax.set_yticks(y_positions)
    ax.set_yticklabels(communities, fontsize=10)
    ax.set_xlabel('% Audience Index', fontsize=12, fontweight='bold')
    ax.set_xticks(np.arange(0, max_scale + 100, 100))

    # Grid and styling
    ax.grid(True, axis='x', color='#CCCCCC', linewidth=0.5, alpha=0.7, zorder=0)
    ax.axvline(x=100, color='black', linewidth=1.5, linestyle='-', alpha=0.8, zorder=2)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    # Legend
    ax.legend(handles=[
        mpatches.Patch(color=bar_color, label='% Audience'),
        mpatches.Patch(color=line_color, label='% Audience Index')
    ], loc='upper center', bbox_to_anchor=(0.5, -0.08), ncol=2)

    # Title
    ax.text(0.98, 0.98, '% Fanbase Composition',
            transform=ax.transAxes, ha='right', va='top',
            fontsize=14, fontweight='normal')

    # Layout and save
    plt.subplots_adjust(left=0.15, right=0.95, top=0.95, bottom=0.12)
    plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()

    print(f"✅ Chart saved to: {save_path}")


def main():
    """Main function to query and generate chart"""
    print("🏀 Utah Jazz Community Index Chart Generator")
    print("=" * 50)

    # Query Snowflake using centralized connection
    df = query_snowflake()

    if df is not None and not df.empty:
        # Print summary
        print(f"\n📊 Data Summary:")
        print(f"Total communities retrieved: {len(df)}")

        # Sort by audience percentage for display
        df_display = df.sort_values('PERC_AUDIENCE_DISPLAY', ascending=False)

        print("\nTop communities by audience percentage:")
        for idx, row in df_display.iterrows():
            print(f"  • {row['COMMUNITY']}: "
                  f"Audience={row['PERC_AUDIENCE_DISPLAY']:.1f}%, "
                  f"Index={row['PERC_INDEX']:.0f}%, "
                  f"Composite={row['COMPOSITE_INDEX']:.0f}")

        # Generate filename with timestamp
        filename = f"jazz_audience_index_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"

        # Create the visualization
        create_audience_index_chart(df, save_path=filename)

        print(f"\n✅ Process complete! Chart saved as: {filename}")

    else:
        print("\n❌ Failed to retrieve data from Snowflake")
        print("Please check your connection settings and query")


if __name__ == "__main__":
    main()