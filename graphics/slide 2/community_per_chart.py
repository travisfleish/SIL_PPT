#!/usr/bin/env python3
"""
Complete script to query Snowflake and generate audience index chart
Updated with correct comparison population value
"""

import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Rectangle
import numpy as np
from dotenv import load_dotenv
import os
from datetime import datetime


def query_snowflake():
    """Connect to Snowflake and get top 10 communities by COMPOSITE_INDEX with enhanced filters"""

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
        return None

    try:
        # Connect to Snowflake
        print("🔄 Connecting to Snowflake...")
        conn = snowflake.connector.connect(**config)
        print("✅ Connected successfully!")

        # Query for top 10 communities with enhanced filters
        query = """
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
            COMPARISON_POPULATION = 'Local Gen Pop (Excl. Jazz)'  -- CORRECTED VALUE
            AND PERC_AUDIENCE >= 0.15  -- Changed from 0.01 to 0.15 (15% minimum)
            AND COMMUNITY NOT IN (
                -- Comprehensive list of major men's sports to exclude
                'General Sports Fans',
                'Fans of Men''s Sports (FOMS)',
                'Fan''s of Men''s Sports (FOMS)',
                'NBA',
                'Basketball',
                'NFL',
                'Football',
                'American Football',
                'College Football',
                'NHL',
                'Hockey',
                'Ice Hockey',
                'MLB',
                'Baseball',
                'MLS',
                'Soccer',
                'Football (Soccer)',
                'Premier League',
                'La Liga',
                'Bundesliga',
                'Serie A',
                'Ligue 1',
                'Champions League',
                'PGA',
                'Golf',
                'NASCAR',
                'Formula 1',
                'F1',
                'Auto Racing',
                'Boxing',
                'MMA',
                'UFC',
                'Wrestling',
                'WWE'
            )
            -- Also filter using pattern matching for more comprehensive exclusion
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
            COMPOSITE_INDEX DESC  -- Select top 10 by COMPOSITE_INDEX
        LIMIT 10
        """

        print("📊 Executing query...")
        print("   - Using ALL_TIME view")
        print("   - Filtering for 'Local Gen Pop (Excl. Jazz)' comparison")
        print("   - Excluding all major men's sports communities")
        print("   - Selecting top 10 by COMPOSITE_INDEX")

        df = pd.read_sql(query, conn)

        # Close connection
        conn.close()
        print("✅ Data retrieved successfully!")

        return df

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return None


def create_audience_index_chart(df, save_path='audience_index_chart.png'):
    """
    Create the audience index chart matching the SIL style.
    Chart displays communities sorted by PERC_AUDIENCE in descending order.
    """

    print("\n🎨 Creating chart...")

    # Sort by PERC_AUDIENCE_DISPLAY descending, then ascending for chart (highest at top)
    df_sorted = df.sort_values('PERC_AUDIENCE_DISPLAY', ascending=False)
    df_sorted = df_sorted.sort_values('PERC_AUDIENCE_DISPLAY', ascending=True)  # Reverse for chart display

    # Extract data
    communities = df_sorted['COMMUNITY'].tolist()
    perc_indices = df_sorted['PERC_INDEX'].tolist()
    perc_audiences = df_sorted['PERC_AUDIENCE_DISPLAY'].tolist()
    composite_indices = df_sorted['COMPOSITE_INDEX'].tolist()

    # Create figure
    fig, ax = plt.subplots(figsize=(10, 8), facecolor='white')

    # Set up positions
    y_positions = np.arange(len(communities))
    bar_height = 0.7

    # Colors
    bar_color = '#5B9BD5'  # Blue
    background_color = '#E0E0E0'  # Light gray
    label_color = '#FFD966'  # Yellow
    line_color = '#404040'  # Darker gray for vertical lines

    # Dynamic scale based on max INDEX value (not audience)
    max_index = max(perc_indices)
    max_scale = int(np.ceil(max_index / 100) * 100)  # Round up to nearest 100

    # Normalize audience percentages to fit within the chart width
    # Bars will be drawn proportionally but not tied to x-axis scale
    max_audience = max(perc_audiences)
    bar_scale_factor = (max_scale * 0.8) / 100  # Use 80% of chart width for bars

    # Draw background bars
    for i, y in enumerate(y_positions):
        bg_rect = Rectangle((0, y - bar_height / 2), max_scale, bar_height,
                            facecolor=background_color, edgecolor='none', zorder=1)
        ax.add_patch(bg_rect)

    # Draw audience percentage bars (scaled independently from x-axis)
    for i, (y, perc_aud) in enumerate(zip(y_positions, perc_audiences)):
        # Scale bar length to use reasonable portion of chart
        bar_length = perc_aud * bar_scale_factor

        bar = Rectangle((0, y - bar_height / 2), bar_length, bar_height,
                        facecolor=bar_color, edgecolor='none', zorder=2)
        ax.add_patch(bar)

        # Add audience percentage text with FULL YELLOW BACKGROUND
        text_x = bar_length + 10  # Just after the bar

        # Create label text with yellow background
        bbox_props = dict(boxstyle="round,pad=0.3",
                          facecolor=label_color,  # Yellow background
                          edgecolor='none',
                          alpha=1.0)

        ax.text(text_x, y, f"{perc_aud:.0f}%",
                va='center', ha='left',
                fontsize=11, fontweight='bold',
                bbox=bbox_props,
                color='black', zorder=4)

    # Add HORIZONTAL lines for index percentage (originating from y-axis)
    for i, perc_idx in enumerate(perc_indices):
        # Line position based on actual index value
        line_x = perc_idx
        y_pos = y_positions[i]

        # Draw horizontal line from y-axis (0) to index position
        ax.plot([0, line_x], [y_pos, y_pos],
                color=line_color, linewidth=2.5, zorder=3)

    # REMOVED index value labels - no longer showing index percentages

    # Customize axes
    ax.set_xlim(0, max_scale)
    ax.set_ylim(-0.5, len(communities) - 0.5)

    # Set y-axis labels
    ax.set_yticks(y_positions)
    ax.set_yticklabels(communities, fontsize=10)

    # Set x-axis
    ax.set_xlabel('% Audience Index', fontsize=12, fontweight='bold')  # Back to Index
    x_ticks = np.arange(0, max_scale + 100, 100)  # Ticks every 100 for index scale
    ax.set_xticks(x_ticks)

    # Add grid
    ax.grid(True, axis='x', color='#CCCCCC', linewidth=0.5, alpha=0.7, zorder=0)
    ax.set_axisbelow(True)

    # Add vertical line at 100% (baseline)
    ax.axvline(x=100, color='black', linewidth=1.5, linestyle='-', alpha=0.8, zorder=2)

    # Customize spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(True)
    ax.spines['left'].set_visible(True)

    # Add legend - SWAPPED to show correct mapping
    index_line = mpatches.Patch(color=line_color, label='% Audience Index')  # Line represents index
    audience_bar = mpatches.Patch(color=bar_color, label='% Audience')  # Bar represents audience

    ax.legend(handles=[audience_bar, index_line],
              loc='upper center', bbox_to_anchor=(0.5, -0.08),
              ncol=2, frameon=True, fancybox=True, shadow=False)

    # Add title in top right
    ax.text(0.98, 0.98, '% Fanbase Composition',
            transform=ax.transAxes,
            ha='right', va='top',
            fontsize=14, fontweight='normal')

    # Adjust layout
    plt.subplots_adjust(left=0.15, right=0.95, top=0.95, bottom=0.12)

    # Save
    plt.savefig(save_path, dpi=300, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close()

    print(f"✅ Chart saved to: {save_path}")


def main():
    """Main function to query and generate chart"""

    print("🏀 Utah Jazz Community Index Chart Generator")
    print("=" * 50)

    # Query Snowflake
    df = query_snowflake()

    if df is not None:
        # Print summary
        print("\n📊 Data Summary:")
        print(f"Total communities retrieved: {len(df)}")
        print("\nTop 10 communities by COMPOSITE_INDEX (sorted by % Audience for display):")

        # Sort by PERC_AUDIENCE_DISPLAY descending for summary display
        df_display = df.sort_values('PERC_AUDIENCE_DISPLAY', ascending=False)

        for idx, row in df_display.iterrows():
            print(
                f"  • {row['COMMUNITY']}: Audience={row['PERC_AUDIENCE_DISPLAY']:.1f}%, Index={row['PERC_INDEX']:.0f}%, Composite={row['COMPOSITE_INDEX']:.0f}")

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"jazz_audience_index_{timestamp}.png"

        # Create chart
        create_audience_index_chart(df, save_path=filename)

        print(f"\n✅ Process complete! Chart saved as: {filename}")
    else:
        print("\n❌ Failed to retrieve data from Snowflake")


if __name__ == "__main__":
    main()