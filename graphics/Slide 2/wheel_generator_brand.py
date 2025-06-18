#!/usr/bin/env python3
"""
Dynamic Fan Wheel Generator
Pulls top communities and their associated merchants from Snowflake
Uses hardcoded approved communities and verbs
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import Wedge, Circle, Polygon
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import matplotlib.font_manager as fm
import os
import numpy as np
import requests
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
from datetime import datetime
import logging
from pathlib import Path
import json

# Import Snowflake connection
from snowflake_connection import query_to_dataframe

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# HARDCODED COMMUNITY CONFIGURATION
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


def format_behavior_text(community, merchant):
    """Format the behavior text for the wheel"""
    verb = APPROVED_COMMUNITIES.get(community.strip(), "Shops at")
    behavior = f"{verb} {merchant}"

    # Format for two lines if needed
    words = behavior.split()
    if len(words) >= 3:
        mid = len(words) // 2
        line1 = ' '.join(words[:mid])
        line2 = ' '.join(words[mid:])
        return f"{line1}\n{line2}"
    elif len(words) == 2:
        return f"{words[0]}\n{words[1]}"
    else:
        return behavior


class BrandfetchAPI:
    """Brandfetch API client for logo downloads"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.brandfetch.io/v2"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json"
        }
        self.cache_dir = Path("cache")
        self.cache_dir.mkdir(exist_ok=True)

    def download_logo(self, merchant_name: str, save_path: str) -> bool:
        """Download logo for a merchant using Brandfetch"""

        # Simple domain generation
        clean_name = merchant_name.lower().replace("'", "").replace(" ", "").replace(",", "")
        domains_to_try = [
            f"{clean_name}.com",
            f"{clean_name}.net",
            f"{clean_name}.org"
        ]

        for domain in domains_to_try:
            url = f"{self.base_url}/brands/{domain}"

            try:
                response = requests.get(url, headers=self.headers, timeout=10)

                if response.status_code == 200:
                    data = response.json()
                    logos = data.get("logos", [])

                    if logos:
                        # Get first available logo
                        logo = logos[0]
                        formats = logo.get("formats", [])

                        if formats:
                            logo_url = formats[0].get("src")
                            if logo_url:
                                logo_response = requests.get(logo_url, timeout=10)
                                if logo_response.status_code == 200:
                                    with open(save_path, 'wb') as f:
                                        f.write(logo_response.content)
                                    logger.info(f"✅ Downloaded logo for {merchant_name}")
                                    return True
            except Exception as e:
                continue

        return False


class DynamicFanWheelGenerator:
    """Generate fan wheel visualization from live Snowflake data"""

    def __init__(self):
        # Brand colors
        self.JAZZ_BLUE = "#1D428A"
        self.JAZZ_YELLOW = "#FFD700"
        self.LIGHT_BLUE = "#4169E1"

        # Initialize Brandfetch API if available
        self.brandfetch_api = None
        brandfetch_key = os.getenv("BRANDFETCH_API_KEY")
        if brandfetch_key:
            self.brandfetch_api = BrandfetchAPI(brandfetch_key)
            logger.info("✓ Brandfetch API initialized")

        # Create directories
        self.logo_dir = Path("logos")
        self.logo_dir.mkdir(exist_ok=True)

        # Setup font
        self.setup_font()

    def setup_font(self):
        """Setup Red Hat Display font"""
        try:
            script_dir = Path(__file__).parent
            font_paths = [
                script_dir / "Red_Hat_Display" / "static" / "RedHatDisplay-Regular.ttf",
                script_dir / "Red_Hat_Display" / "static" / "RedHatDisplay-Bold.ttf",
                Path("Red_Hat_Display/static/RedHatDisplay-Regular.ttf"),
            ]

            loaded_fonts = []
            for font_path in font_paths:
                if font_path.exists():
                    fm.fontManager.addfont(str(font_path))
                    loaded_fonts.append(str(font_path))
                    logger.info(f"✓ Loaded font: {font_path.name}")
                    break

            if loaded_fonts:
                self.font_name = 'Red Hat Display'
                self.font_regular = fm.FontProperties(fname=loaded_fonts[0])
                self.font_bold = fm.FontProperties(fname=loaded_fonts[-1]) if len(
                    loaded_fonts) > 1 else self.font_regular
            else:
                logger.warning("Red Hat Display not found. Using default font.")
                self.font_name = 'sans-serif'
                self.font_regular = None
                self.font_bold = None

        except Exception as e:
            logger.warning(f"Error setting up font: {e}")
            self.font_name = 'sans-serif'
            self.font_regular = None
            self.font_bold = None

    def fetch_wheel_data(self):
        """Fetch top communities and their top merchants from Snowflake"""

        # Get SQL clause for approved communities
        approved_communities_sql = get_approved_communities_sql()

        # Query only approved communities
        communities_query = f"""
        SELECT 
            COMMUNITY,
            PERC_AUDIENCE,
            COMPOSITE_INDEX
        FROM 
            V_UTAH_JAZZ_SIL_COMMUNITY_INDEXING_ALL_TIME
        WHERE 
            COMPARISON_POPULATION = 'Local Gen Pop (Excl. Jazz)'
            AND PERC_AUDIENCE >= 0.15
            AND COMMUNITY IN ({approved_communities_sql})
        ORDER BY COMPOSITE_INDEX DESC
        LIMIT 10
        """

        try:
            logger.info("Fetching approved communities from Snowflake...")
            top_communities_df = query_to_dataframe(communities_query)

            if top_communities_df.empty:
                raise ValueError("No approved communities found")

            communities = top_communities_df['COMMUNITY'].tolist()
            logger.info(f"Found {len(communities)} top communities")

            # Get top merchant for each community
            merchants_query = f"""
            WITH top_communities AS (
                {communities_query}
            ),
            ranked_merchants AS (
                SELECT 
                    tc.COMMUNITY,
                    m.MERCHANT,
                    m.CATEGORY,
                    m.SUBCATEGORY,
                    m.PERC_INDEX,
                    m.AUDIENCE_TOTAL_SPEND,
                    m.AUDIENCE_COUNT,
                    ROW_NUMBER() OVER (PARTITION BY tc.COMMUNITY ORDER BY m.PERC_INDEX DESC) as rn
                FROM top_communities tc
                JOIN V_SIL_COMMUNITY_MERCHANT_INDEXING_ALL_TIME m
                    ON tc.COMMUNITY = m.COMMUNITY
                WHERE m.COMPARISON_POPULATION = 'Local Gen Pop (Excl. Jazz)'
                    AND m.AUDIENCE_COUNT > 10
            )
            SELECT 
                COMMUNITY,
                MERCHANT,
                CATEGORY,
                SUBCATEGORY,
                PERC_INDEX
            FROM ranked_merchants 
            WHERE rn = 1
            ORDER BY PERC_INDEX DESC
            """

            logger.info("Fetching top merchants for each community...")
            wheel_data_df = query_to_dataframe(merchants_query)

            # Generate behaviors using hardcoded verbs
            wheel_data_df['behavior'] = wheel_data_df.apply(
                lambda row: format_behavior_text(row['COMMUNITY'], row['MERCHANT']),
                axis=1
            )

            return wheel_data_df

        except Exception as e:
            logger.error(f"Error fetching data from Snowflake: {e}")
            raise

    def download_or_generate_logo(self, merchant, save_path):
        """Try Brandfetch first, then Clearbit, then create simple text fallback"""

        # Try Brandfetch if available
        if self.brandfetch_api:
            if self.brandfetch_api.download_logo(merchant, save_path):
                return True

        # Fallback to Clearbit
        if self.download_logo_clearbit(merchant, save_path):
            return True

        # Final fallback to letter logo
        self.create_letter_logo(merchant, save_path)
        return True

    def download_logo_clearbit(self, merchant, save_path):
        """Try to download logo using Clearbit API"""

        clean_name = merchant.lower().replace("'", "").replace(" ", "").replace(",", "")
        domains_to_try = [f"{clean_name}.com", f"{clean_name}.net"]

        for domain in domains_to_try:
            try:
                url = f"https://logo.clearbit.com/{domain}"
                response = requests.get(url, timeout=5)

                if response.status_code == 200 and 'image' in response.headers.get('content-type', ''):
                    with open(save_path, "wb") as f:
                        f.write(response.content)
                    logger.info(f"✓ Downloaded logo for {merchant} via Clearbit")
                    return True
            except:
                continue

        return False

    def create_letter_logo(self, merchant, save_path):
        """Create a simple letter-based logo"""

        img = Image.new('RGBA', (400, 400), (255, 255, 255, 0))
        draw = ImageDraw.Draw(img)

        letter = merchant.strip()[0].upper()

        # Draw circle background
        draw.ellipse([50, 50, 350, 350], fill=(240, 240, 240, 255))

        # Draw letter
        font_size = 200
        # Approximate centering
        x, y = 200, 200
        draw.text((x, y), letter, fill=(100, 100, 100, 255), anchor="mm")

        img.save(save_path)
        logger.info(f"Created letter logo for {merchant}")

    def download_jazz_logo(self):
        """Download Utah Jazz logo"""
        try:
            url = "https://logo.clearbit.com/nba.com"
            response = requests.get(url, timeout=5)

            if response.status_code == 200:
                img = Image.open(BytesIO(response.content))
                if img.mode != 'RGBA':
                    img = img.convert('RGBA')
                return img
        except:
            pass

        return None

    def generate_wheel(self, wheel_data_df, output_file="dynamic_fan_wheel.png"):
        """Generate the wheel visualization"""

        # Create figure
        fig = plt.figure(figsize=(12, 12), facecolor='white')
        ax = fig.add_subplot(111, aspect='equal')
        ax.set_xlim(-6, 6)
        ax.set_ylim(-6, 6)
        ax.axis('off')

        num_items = len(wheel_data_df)
        angle_step = 360 / num_items

        # Parameters
        outer_radius = 5.0
        logo_radius = 2.8
        middle_radius = logo_radius
        inner_radius = 1.6

        # Download/generate logos
        for idx, row in wheel_data_df.iterrows():
            merchant = row['MERCHANT']
            filename = merchant.lower().replace(' ', '_').replace("'", '').replace(",", "") + ".png"
            filepath = self.logo_dir / filename

            if not filepath.exists():
                self.download_or_generate_logo(merchant, filepath)

            wheel_data_df.at[idx, 'logo_path'] = str(filepath)

        # Download Jazz logo
        jazz_logo = self.download_jazz_logo()

        # Draw wedges
        for i in range(num_items):
            start_angle = i * angle_step - 90
            end_angle = (i + 1) * angle_step - 90

            # Full wedge
            full_wedge = Wedge((0, 0), outer_radius, start_angle, end_angle,
                               width=outer_radius,
                               facecolor=self.JAZZ_BLUE,
                               edgecolor='none',
                               zorder=1)
            ax.add_patch(full_wedge)

            # Outer ring
            outer_ring = Wedge((0, 0), outer_radius, start_angle, end_angle,
                               width=outer_radius - middle_radius,
                               facecolor=self.LIGHT_BLUE,
                               edgecolor='none',
                               zorder=2)
            ax.add_patch(outer_ring)

        # Add dividing lines
        for i in range(num_items):
            angle = i * angle_step - 90
            angle_rad = np.deg2rad(angle)

            x_inner = inner_radius * np.cos(angle_rad)
            y_inner = inner_radius * np.sin(angle_rad)
            x_outer = outer_radius * np.cos(angle_rad)
            y_outer = outer_radius * np.sin(angle_rad)

            ax.plot([x_inner, x_outer], [y_inner, y_outer],
                    color='white', linewidth=8, zorder=15)

        # Add arrows
        for i in range(num_items):
            arrow_angle_deg = i * angle_step - 90
            arrow_angle = np.deg2rad(arrow_angle_deg)

            arrow_r = 4.0
            arrow_x = arrow_r * np.cos(arrow_angle)
            arrow_y = arrow_r * np.sin(arrow_angle)

            # White circle background
            circle_bg = Circle((arrow_x, arrow_y), 0.3,
                               facecolor='white',
                               edgecolor='none',
                               zorder=16)
            ax.add_patch(circle_bg)

            # Yellow arrow
            arrow_size = 0.15
            arrow_direction = arrow_angle - np.pi / 2

            tip_x = arrow_x + arrow_size * np.cos(arrow_direction)
            tip_y = arrow_y + arrow_size * np.sin(arrow_direction)

            base_center_x = arrow_x - arrow_size * 0.5 * np.cos(arrow_direction)
            base_center_y = arrow_y - arrow_size * 0.5 * np.sin(arrow_direction)

            base_offset = arrow_size * 0.4
            base1_x = base_center_x + base_offset * np.cos(arrow_direction + np.pi / 2)
            base1_y = base_center_y + base_offset * np.sin(arrow_direction + np.pi / 2)
            base2_x = base_center_x - base_offset * np.cos(arrow_direction + np.pi / 2)
            base2_y = base_center_y - base_offset * np.sin(arrow_direction + np.pi / 2)

            arrow = Polygon([(tip_x, tip_y), (base1_x, base1_y), (base2_x, base2_y)],
                            facecolor=self.JAZZ_YELLOW, edgecolor=self.JAZZ_YELLOW, zorder=17)
            ax.add_patch(arrow)

        # Center circle
        center_circle = Circle((0, 0), inner_radius,
                               facecolor='black',
                               edgecolor=self.JAZZ_YELLOW,
                               linewidth=5,
                               zorder=20)
        ax.add_patch(center_circle)

        # Add Jazz logo if available
        text_y_position = 0
        if jazz_logo:
            try:
                jazz_array = np.array(jazz_logo)
                logo_size = inner_radius * 0.4
                imagebox = OffsetImage(jazz_array, zoom=logo_size / 2)
                ab = AnnotationBbox(imagebox, (0, 0.4),
                                    frameon=False, zorder=21)
                ax.add_artist(ab)
                text_y_position = -0.6
            except Exception as e:
                logger.warning(f"Could not add Jazz logo: {e}")

        # Center text
        ax.text(0, text_y_position, "THE JAZZ FAN",
                ha='center', va='center',
                fontsize=16, fontweight='bold',
                color='white', zorder=22)

        # Add logos and text
        for i, row in wheel_data_df.iterrows():
            center_angle = i * angle_step + angle_step / 2 - 90
            angle_rad = np.deg2rad(center_angle)

            # Logo position
            logo_x = logo_radius * np.cos(angle_rad)
            logo_y = logo_radius * np.sin(angle_rad)

            # Logo background
            logo_bg = Circle((logo_x, logo_y), 0.55,
                             facecolor='white',
                             edgecolor='none',
                             zorder=5)
            ax.add_patch(logo_bg)

            # Add logo
            try:
                pil_img = Image.open(row['logo_path'])
                if pil_img.mode != 'RGBA':
                    pil_img = pil_img.convert('RGBA')

                imagebox = OffsetImage(pil_img, zoom=0.35)
                ab = AnnotationBbox(imagebox, (logo_x, logo_y),
                                    frameon=False, zorder=6)
                ax.add_artist(ab)
            except:
                # Fallback to initials
                initials = ''.join([word[0].upper() for word in row['MERCHANT'].split()[:2]])
                ax.text(logo_x, logo_y, initials,
                        ha='center', va='center',
                        fontsize=20, fontweight='bold',
                        color='gray', zorder=6)

            # Add behavior text
            text_radius_center = (middle_radius + outer_radius) / 2
            text_x = text_radius_center * np.cos(angle_rad)
            text_y = text_radius_center * np.sin(angle_rad)

            ax.text(text_x, text_y, row['behavior'],
                    ha='center', va='center',
                    fontsize=14, fontweight='bold',
                    color='white',
                    rotation=0,
                    linespacing=0.8,
                    zorder=7)

        # Save
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight',
                    facecolor='white', edgecolor='none')
        plt.close()

        logger.info(f"✅ Fan wheel saved as {output_file}")

        # Save data summary
        summary_data = wheel_data_df[['COMMUNITY', 'MERCHANT', 'PERC_INDEX', 'behavior']].copy()
        summary_data.to_csv('fan_wheel_data_summary.csv', index=False)

        return output_file


def main():
    """Main execution function"""

    print("🏀 Dynamic Fan Wheel Generator")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    generator = DynamicFanWheelGenerator()

    try:
        # Fetch data from Snowflake
        print("\n📊 Fetching data from Snowflake...")
        wheel_data = generator.fetch_wheel_data()

        print(f"\n✅ Retrieved {len(wheel_data)} merchant-community pairs")
        print("\nTop merchants by community:")
        for _, row in wheel_data.iterrows():
            print(f"  • {row['COMMUNITY']}: {row['MERCHANT']} (Index: {row['PERC_INDEX']:.0f}%)")

        # Generate wheel
        print("\n🎨 Generating fan wheel visualization...")
        output_file = f"jazz_fan_wheel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        generator.generate_wheel(wheel_data, output_file)

        print(f"\n✅ Process complete!")
        print(f"   Fan wheel: {output_file}")
        print(f"   Data summary: fan_wheel_data_summary.csv")

    except Exception as e:
        logger.error(f"Failed to generate fan wheel: {e}")
        raise


if __name__ == "__main__":
    main()