#!/usr/bin/env python3
"""
Complete script to query Snowflake and generate audience index chart
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
    """Connect to Snowflake and get top 10 communities by COMPOSITE_INDEX with filters"""

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

        # Query for top 10 communities with filters
        query = """
        SELECT 
            COMMUNITY,
            PERC_INDEX,
            COMPOSITE_INDEX,
            PERC_AUDIENCE * 100 AS PERC_AUDIENCE_DISPLAY,
            AUDIENCE_COUNT,
            TOTAL_AUDIENCE_COUNT
        FROM 
            V_UTAH_JAZZ_SIL_COMMUNITY_INDEXING_YOY
        WHERE 
            TRANSACTION_YEAR = '2025-01-01'
            AND COMPARISON_POPULATION = 'General Population'
            AND PERC_AUDIENCE >= 0.01  -- Changed from 0.15 to 0.01 (1% minimum)
            AND COMMUNITY NOT IN (
                'General Sports Fans',
                'Fans of Men''s Sports (FOMS)',
                'Fan''s of Men''s Sports (FOMS)',
                'NBA',
                'Basketball'
            )
        ORDER BY 
            COMPOSITE_INDEX DESC  -- Changed from PERC_INDEX to COMPOSITE_INDEX
        LIMIT 10
        """

        print("📊 Executing query...")
        print("   - Filtering for minimum 1% audience")
        print("   - Excluding: General Sports Fans, FOMS, NBA, Basketball")
        print("   - Sorting by COMPOSITE_INDEX")

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
    """

    print("\n🎨 Creating chart...")

    # Sort by COMPOSITE_INDEX ascending for correct display order (highest at top)
    df_sorted = df.sort_values('COMPOSITE_INDEX', ascending=True)

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

    # Dynamic scale based on max value
    max_index = max(perc_indices)
    max_scale = int(np.ceil(max_index / 1000) * 1000)  # Round up to nearest 1000

    # Draw background bars
    for i, y in enumerate(y_positions):
        bg_rect = Rectangle((0, y - bar_height / 2), max_scale, bar_height,
                            facecolor=background_color, edgecolor='none', zorder=1)
        ax.add_patch(bg_rect)

    # Draw index bars
    bars = ax.barh(y_positions, perc_indices, height=bar_height,
                   color=bar_color, edgecolor='none', zorder=2)

    # Add vertical lines for audience percentage
    # Position lines based on audience percentage relative to max scale
    for i, (bar, perc_aud) in enumerate(zip(bars, perc_audiences)):
        # Scale the audience percentage to the chart scale
        # If max audience is ~25%, position it proportionally on the x-axis
        line_x = (perc_aud / 100) * max_scale  # Convert percentage to chart scale

        # Draw vertical line
        y_bottom = bar.get_y()
        y_top = bar.get_y() + bar.get_height()

        ax.plot([line_x, line_x], [y_bottom, y_top],
                color=line_color, linewidth=2.5, zorder=3)

    # Add value labels with yellow background - NOW SHOWING PERC_AUDIENCE
    for i, (bar, perc_aud) in enumerate(zip(bars, perc_audiences)):
        # Position label near the vertical line
        label_x = (perc_aud / 100) * max_scale + 50  # Just after the vertical line
        label_y = bar.get_y() + bar.get_height() / 2

        # Create label text - show audience percentage
        label_text = f"{perc_aud:.0f}%"

        # Add yellow background box
        bbox_props = dict(boxstyle="round,pad=0.3",
                          facecolor=label_color,
                          edgecolor='none',
                          alpha=1.0)

        ax.text(label_x, label_y, label_text,
                va='center', ha='left',
                fontsize=11, fontweight='bold',
                bbox=bbox_props, zorder=4)

    # Customize axes
    ax.set_xlim(0, max_scale)
    ax.set_ylim(-0.5, len(communities) - 0.5)

    # Set y-axis labels
    ax.set_yticks(y_positions)
    ax.set_yticklabels(communities, fontsize=10)

    # Set x-axis
    ax.set_xlabel('% Audience Index', fontsize=12, fontweight='bold')
    x_ticks = np.arange(0, max_scale + 1000, 1000)
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

    # Add legend - CORRECTED to show line color for % Audience
    audience_patch = mpatches.Patch(color=line_color, label='% Audience')  # Changed to line_color
    index_patch = mpatches.Patch(color=bar_color, label='% Audience Index')

    ax.legend(handles=[audience_patch, index_patch],
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
        print("\nTop communities by composite index (with filters applied):")
        for idx, row in df.head(10).iterrows():
            print(
                f"  • {row['COMMUNITY']}: Composite={row['COMPOSITE_INDEX']:.0f}, Index={row['PERC_INDEX']:.0f}%, Audience={row['PERC_AUDIENCE_DISPLAY']:.1f}%")

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