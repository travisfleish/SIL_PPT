#!/usr/bin/env python3
"""
Dynamic Fan Wheel Generator
Pulls top communities and their associated merchants from Snowflake
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
from openai import OpenAI
from dotenv import load_dotenv
from pathlib import Path

# Import Snowflake connection
from snowflake_connection import query_to_dataframe

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class DynamicFanWheelGenerator:
    """Generate fan wheel visualization from live Snowflake data"""

    def __init__(self):
        # Brand colors
        self.JAZZ_BLUE = "#1D428A"
        self.JAZZ_YELLOW = "#FFD700"
        self.LIGHT_BLUE = "#4169E1"

        # Initialize OpenAI client
        self.openai_client = None
        if os.getenv("OPENAI_API_KEY"):
            self.openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        # Create directories
        self.logo_dir = Path("logos")
        self.logo_dir.mkdir(exist_ok=True)

        # Setup font
        self.setup_font()

    def setup_font(self):
        """Setup Red Hat Display font"""
        try:
            # Get the script's directory
            script_dir = Path(__file__).parent

            # Try to load from various possible locations
            font_paths = [
                # Static fonts from script directory (more reliable with matplotlib)
                script_dir / "Red_Hat_Display" / "static" / "RedHatDisplay-Regular.ttf",
                script_dir / "Red_Hat_Display" / "static" / "RedHatDisplay-Bold.ttf",
                script_dir / "Red_Hat_Display" / "static" / "RedHatDisplay-Medium.ttf",
                # Variable font as fallback
                script_dir / "Red_Hat_Display" / "RedHatDisplay-VariableFont_wght.ttf",
                # Direct file if moved to script directory
                script_dir / "RedHatDisplay-Regular.ttf",
                # From current working directory
                Path("Red_Hat_Display/static/RedHatDisplay-Regular.ttf"),
            ]

            font_loaded = False
            loaded_fonts = []

            for font_path in font_paths:
                if font_path.exists():
                    # Add the font to matplotlib's font manager
                    fm.fontManager.addfont(str(font_path))
                    loaded_fonts.append(str(font_path))

                    logger.info(f"✓ Loaded font file: {font_path.name}")
                    print(f"✓ Successfully loaded font file: {font_path.name}")

                    # Load bold variant if we loaded regular
                    if "Regular" in str(font_path) and not font_loaded:
                        font_loaded = True
                        bold_path = font_path.parent / "RedHatDisplay-Bold.ttf"
                        if bold_path.exists():
                            fm.fontManager.addfont(str(bold_path))
                            loaded_fonts.append(str(bold_path))
                            logger.info(f"✓ Also loaded Bold variant")
                        break

            if loaded_fonts:
                # Set the font name
                self.font_name = 'Red Hat Display'

                # Create font properties for direct use
                self.font_regular = fm.FontProperties(fname=loaded_fonts[0])
                self.font_bold = fm.FontProperties(fname=loaded_fonts[-1]) if len(
                    loaded_fonts) > 1 else self.font_regular

                # Set as default font for matplotlib
                import matplotlib.pyplot as plt
                plt.rcParams['font.sans-serif'] = ['Red Hat Display'] + plt.rcParams['font.sans-serif']
                plt.rcParams['font.family'] = 'sans-serif'

                print(f"✓ Red Hat Display font configured successfully")
            else:
                # Fallback
                logger.warning("Red Hat Display not found. Using default font.")
                print("⚠️  Red Hat Display font not found - using default font")
                self.font_name = 'sans-serif'
                self.font_regular = None
                self.font_bold = None

        except Exception as e:
            logger.warning(f"Error setting up font: {e}")
            print(f"❌ Error loading font: {e}")
            self.font_name = 'sans-serif'
            self.font_regular = None
            self.font_bold = None

    def fetch_wheel_data(self):
        """Fetch top communities and their top merchants from Snowflake"""

        # First get top 10 communities (same logic as community chart)
        communities_query = """
        SELECT 
            COMMUNITY,
            PERC_AUDIENCE,
            COMPOSITE_INDEX
        FROM 
            V_UTAH_JAZZ_SIL_COMMUNITY_INDEXING_ALL_TIME
        WHERE 
            COMPARISON_POPULATION = 'Local Gen Pop (Excl. Jazz)'
            AND PERC_AUDIENCE >= 0.15
            AND COMMUNITY NOT IN (
                'General Sports Fans', 'Fans of Men''s Sports (FOMS)', 'Fan''s of Men''s Sports (FOMS)',
                'NBA', 'Basketball', 'NFL', 'Football', 'American Football', 'College Football',
                'NHL', 'Hockey', 'Ice Hockey', 'MLB', 'Baseball', 'MLS', 'Soccer', 'Football (Soccer)',
                'Premier League', 'La Liga', 'Bundesliga', 'Serie A', 'Ligue 1', 'Champions League',
                'PGA', 'Golf', 'NASCAR', 'Formula 1', 'F1', 'Auto Racing', 'Boxing', 'MMA', 'UFC',
                'Wrestling', 'WWE'
            )
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
        ORDER BY COMPOSITE_INDEX DESC
        LIMIT 10
        """

        try:
            logger.info("Fetching top communities from Snowflake...")
            top_communities_df = query_to_dataframe(communities_query)

            if top_communities_df.empty:
                raise ValueError("No communities found")

            communities = top_communities_df['COMMUNITY'].tolist()
            logger.info(f"Found {len(communities)} top communities")

            # Now get top merchant for each community
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
                    AND m.AUDIENCE_COUNT > 10  -- Minimum threshold for relevance
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

            logger.info(f"Retrieved {len(wheel_data_df)} merchant records")

            # Generate behaviors using OpenAI
            wheel_data_df['behavior'] = wheel_data_df.apply(
                lambda row: self.generate_behavior_text(row['MERCHANT'], row['CATEGORY'], row['SUBCATEGORY']),
                axis=1
            )

            return wheel_data_df

        except Exception as e:
            logger.error(f"Error fetching data from Snowflake: {e}")
            raise

    def generate_behavior_text(self, merchant, category, subcategory):
        """Generate dynamic behavior text using OpenAI"""

        if not self.openai_client:
            # Fallback to simple generation
            return self.simple_behavior_text(merchant, category)

        try:
            prompt = f"""
            Generate a short 2-3 word action phrase for a fan who shops at {merchant}.
            Category: {category}
            Subcategory: {subcategory if subcategory else 'N/A'}

            Examples:
            - Shops at AutoZone
            - Dines at Applebee's
            - Banks with Chase
            - Fills up at Shell

            Rules:
            1. Maximum 3 words
            2. Start with an action verb
            3. Use "at" or "with" as appropriate
            4. Keep it natural and conversational
            5. NO QUOTES in the response
            6. Don't overuse "buys at", use alternative specific to a brand (i.e., "eats at" for restaurants)

            Return only the phrase, nothing else.
            """

            response = self.openai_client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5,
                max_tokens=20
            )

            behavior = response.choices[0].message.content.strip()

            # Ensure it fits on two lines for the wheel
            words = behavior.split()
            if len(words) > 3:
                behavior = " ".join(words[:3])

            # Format for two lines to prevent bleeding
            if len(words) >= 3:
                # For 3+ words, split into two lines more evenly
                mid = len(words) // 2
                line1 = ' '.join(words[:mid])
                line2 = ' '.join(words[mid:])
                behavior = f"{line1}\n{line2}"
            elif len(words) == 2:
                behavior = f"{words[0]}\n{words[1]}"
            else:
                behavior = text

            return behavior

        except Exception as e:
            logger.warning(f"OpenAI generation failed, using fallback: {e}")
            return self.simple_behavior_text(merchant, category)

    def simple_behavior_text(self, merchant, category):
        """Simple fallback behavior text generation"""

        # Category-based verb mapping
        verb_map = {
            'Restaurant': 'Dines at',
            'Retail': 'Shops at',
            'Grocery': 'Shops at',
            'Gas': 'Fills up at',
            'Bank': 'Banks with',
            'Entertainment': 'Enjoys',
            'Travel': 'Travels with',
            'Hotel': 'Stays at',
            'Insurance': 'Insured by',
            'Telecom': 'Connects with',
            'Utilities': 'Powered by',
            'Healthcare': 'Cares with',
            'Fitness': 'Works out at',
            'Education': 'Learns at',
            'Automotive': 'Services at'
        }

        # Find matching verb
        verb = 'Shops at'  # default
        if category:
            for key, value in verb_map.items():
                if key.lower() in category.lower():
                    verb = value
                    break

        # Format for two lines
        words = f"{verb} {merchant}".split()
        if len(words) >= 3:
            return f"{words[0]} {words[1]}\n{' '.join(words[2:])}"
        elif len(words) == 2:
            return f"{words[0]}\n{words[1]}"
        else:
            return merchant

    def download_or_generate_logo(self, merchant, save_path):
        """Try Clearbit first, then create simple text fallback"""

        # First try Clearbit
        if self.download_logo_clearbit(merchant, save_path):
            return True

        # Fallback to simple letter logo
        self.create_letter_logo(merchant, save_path)
        return True

    def download_logo_clearbit(self, merchant, save_path):
        """Try to download logo using Clearbit API"""

        logger.info(f"Attempting Clearbit logo download for {merchant}")

        # Clean merchant name for domain search
        clean_name = merchant.lower().replace("'", "").replace(" ", "").replace(",", "").replace(".", "")

        # Try common domain patterns
        domains_to_try = [
            f"{clean_name}.com",
            f"www.{clean_name}.com",
            f"{clean_name}.net",
            f"{clean_name}.org"
        ]

        for domain in domains_to_try:
            try:
                url = f"https://logo.clearbit.com/{domain}"
                response = requests.get(url, timeout=5, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })

                if response.status_code == 200 and 'image' in response.headers.get('content-type', ''):
                    with open(save_path, "wb") as f:
                        f.write(response.content)
                    logger.info(f"✓ Downloaded logo for {merchant} from {domain}")
                    return True

            except Exception as e:
                continue

        logger.info(f"✗ Clearbit logo not found for {merchant}")
        return False

    def download_jazz_logo(self):
        """Download Utah Jazz logo using Clearbit"""
        try:
            # Try official NBA team domain patterns
            domains_to_try = [
                "nba.com/jazz",
                "utahjazz.com",
                "jazz.com"
            ]

            for domain in domains_to_try:
                url = f"https://logo.clearbit.com/{domain}"
                response = requests.get(url, timeout=5, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })

                if response.status_code == 200 and 'image' in response.headers.get('content-type', ''):
                    # Process the logo for transparency
                    img = Image.open(BytesIO(response.content))
                    if img.mode != 'RGBA':
                        img = img.convert('RGBA')

                    # Make white pixels transparent for better overlay on black
                    datas = img.getdata()
                    newData = []
                    for item in datas:
                        # If pixel is white or near-white, make it transparent
                        if item[0] > 200 and item[1] > 200 and item[2] > 200:
                            newData.append((255, 255, 255, 0))
                        else:
                            newData.append(item)

                    img.putdata(newData)
                    logger.info(f"✓ Downloaded Jazz logo from {domain}")
                    return img

            logger.warning("Could not download Jazz logo from Clearbit")
            return None

        except Exception as e:
            logger.error(f"Error downloading Jazz logo: {e}")
            return None

    def create_letter_logo(self, merchant, save_path):
        """Create a simple letter-based logo with just the first character"""

        # Create transparent background
        img = Image.new('RGBA', (400, 400), (255, 255, 255, 0))
        draw = ImageDraw.Draw(img)

        # Get first letter of merchant name
        letter = merchant.strip()[0].upper()

        # Draw a subtle gray circle background (optional)
        # draw.ellipse([50, 50, 350, 350], fill=(240, 240, 240, 255))

        # Draw the letter in dark gray
        # Using a large font size for visibility
        # Note: This is approximate positioning. In production, use ImageFont.truetype()
        font_size = 250
        # Approximate centering
        x = 200
        y = 200

        # Draw letter
        draw.text((x, y), letter, fill=(100, 100, 100, 255), anchor="mm")

        img.save(save_path)
        logger.info(f"Created letter logo for {merchant} (letter: {letter})")

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
        logo_radius = 2.8  # Reduced from 3.0 - logos closer to center
        middle_radius = logo_radius
        inner_radius = 1.6  # Smaller center circle

        # Download/generate logos
        for idx, row in wheel_data_df.iterrows():
            merchant = row['MERCHANT']
            filename = merchant.lower().replace(' ', '_').replace("'", '').replace(",", "") + ".png"
            filepath = self.logo_dir / filename

            if not filepath.exists():
                self.download_or_generate_logo(merchant, filepath)

            wheel_data_df.at[idx, 'logo_path'] = str(filepath)

        # Download Jazz logo for center
        jazz_logo = self.download_jazz_logo()

        # Draw wedges (same visualization logic as before)
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

        # Add dividing lines - make them wider
        for i in range(num_items):
            angle = i * angle_step - 90
            angle_rad = np.deg2rad(angle)

            x_inner = inner_radius * np.cos(angle_rad)
            y_inner = inner_radius * np.sin(angle_rad)
            x_outer = outer_radius * np.cos(angle_rad)
            y_outer = outer_radius * np.sin(angle_rad)

            ax.plot([x_inner, x_outer], [y_inner, y_outer],
                    color='white', linewidth=8, zorder=15)  # Increased from 5 to 8

        # Add arrows (positioned on dividing lines between segments)
        for i in range(num_items):
            # Position arrow on the dividing line between segments
            arrow_angle_deg = i * angle_step - 90  # On the dividing line
            arrow_angle = np.deg2rad(arrow_angle_deg)

            # Arrow position - further out to match SKY FAN reference
            arrow_r = 4.0  # Updated to 4.0 as requested
            arrow_x = arrow_r * np.cos(arrow_angle)
            arrow_y = arrow_r * np.sin(arrow_angle)

            # Add white circle background - larger to match SKY FAN
            circle_bg = Circle((arrow_x, arrow_y), 0.3,  # Increased to 0.3 as requested
                               facecolor='white',
                               edgecolor='none',
                               zorder=16)  # High z-order to be above dividing lines
            ax.add_patch(circle_bg)

            # Create yellow arrow pointing clockwise
            arrow_size = 0.15  # Increased to 0.25 as requested
            arrow_direction = arrow_angle - np.pi / 2  # -90 degrees for clockwise

            # Arrow vertices forming a triangle
            tip_x = arrow_x + arrow_size * np.cos(arrow_direction)
            tip_y = arrow_y + arrow_size * np.sin(arrow_direction)

            base_center_x = arrow_x - arrow_size * 0.5 * np.cos(arrow_direction)
            base_center_y = arrow_y - arrow_size * 0.5 * np.sin(arrow_direction)

            base_offset = arrow_size * 0.4  # Slightly wider arrow
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
        if jazz_logo:
            try:
                # Convert PIL image to numpy array
                logo_array = np.array(jazz_logo)
                # Scale logo to fit nicely in smaller center circle
                logo_size = inner_radius * 0.4  # Reduced from 0.5 for smaller circle
                imagebox = OffsetImage(logo_array, zoom=logo_size / 2)
                # Position slightly above center to leave room for text
                ab = AnnotationBbox(imagebox, (0, 0.4),  # Adjusted position
                                    frameon=False, zorder=21)
                ax.add_artist(ab)

                # Adjust text position to be below logo
                text_y_position = -0.6  # Adjusted for smaller circle
            except Exception as e:
                logger.warning(f"Could not add Jazz logo: {e}")
                text_y_position = 0
        else:
            text_y_position = 0

        # Center text
        if self.font_bold:
            ax.text(0, text_y_position, "THE JAZZ FAN",
                    ha='center', va='center',
                    fontsize=30, fontweight='bold',
                    fontproperties=self.font_bold,
                    color='white', zorder=22)
        else:
            ax.text(0, text_y_position, "THE JAZZ FAN",
                    ha='center', va='center',
                    fontsize=16, fontweight='bold',
                    family='Red Hat Display',
                    color='white', zorder=22)

        # Add logos and text
        for i, row in wheel_data_df.iterrows():
            center_angle = i * angle_step + angle_step / 2 - 90
            angle_rad = np.deg2rad(center_angle)

            # Logo position
            logo_x = logo_radius * np.cos(angle_rad)
            logo_y = logo_radius * np.sin(angle_rad)

            # Logo background - smaller circle
            logo_bg = Circle((logo_x, logo_y), 0.55,  # Reduced from 0.65
                             facecolor='white',
                             edgecolor='none',
                             zorder=5)
            ax.add_patch(logo_bg)

            # Add logo
            try:
                pil_img = Image.open(row['logo_path'])
                if pil_img.mode != 'RGBA':
                    pil_img = pil_img.convert('RGBA')

                # Check if logo has a colored background
                # Get the average color of the corners to detect background
                width, height = pil_img.size
                corners = [
                    pil_img.getpixel((0, 0)),
                    pil_img.getpixel((width - 1, 0)),
                    pil_img.getpixel((0, height - 1)),
                    pil_img.getpixel((width - 1, height - 1))
                ]

                # Check if corners have consistent non-white color
                has_colored_bg = False
                bg_color = None
                if all(len(c) >= 3 for c in corners):  # Ensure we have RGB values
                    # Check if all corners are similar and not white
                    first_corner = corners[0][:3]
                    if all(abs(c[i] - first_corner[i]) < 30 for c in corners for i in range(3)):
                        # Corners are similar
                        if not all(c > 240 for c in first_corner):  # Not white
                            has_colored_bg = True
                            bg_color = first_corner

                # If logo has colored background, extend it
                if has_colored_bg and bg_color:
                    # Remove the white circle background
                    logo_bg.remove()

                    # Add colored circle background
                    logo_bg_colored = Circle((logo_x, logo_y), 0.55,  # Match reduced size
                                             facecolor=tuple(c / 255 for c in bg_color) + (1,),
                                             edgecolor='none',
                                             zorder=5)
                    ax.add_patch(logo_bg_colored)

                img_array = np.array(pil_img)
                imagebox = OffsetImage(img_array, zoom=0.38)  # Reduced from 0.45
                ab = AnnotationBbox(imagebox, (logo_x, logo_y),
                                    frameon=False, zorder=6)
                ax.add_artist(ab)
            except Exception as e:
                # Fallback to initials
                initials = ''.join([word[0].upper() for word in row['MERCHANT'].split()[:2]])
                if self.font_bold:
                    ax.text(logo_x, logo_y, initials,
                            ha='center', va='center',
                            fontsize=12, fontweight='bold',
                            fontproperties=self.font_bold,
                            color='gray', zorder=6)
                else:
                    ax.text(logo_x, logo_y, initials,
                            ha='center', va='center',
                            fontsize=12, fontweight='bold',
                            family='Red Hat Display',
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
        logger.info("💾 Saved data summary to fan_wheel_data_summary.csv")

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