import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import Wedge, Circle, Polygon
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import os
import numpy as np
import requests
from PIL import Image, ImageDraw
from io import BytesIO

# Create directories
logo_dir = "logos"
os.makedirs(logo_dir, exist_ok=True)

# Debug: Show current working directory
print(f"Current working directory: {os.getcwd()}")
print(f"Logo directory path: {os.path.abspath(logo_dir)}")
print(f"Files in logo directory: {os.listdir(logo_dir) if os.path.exists(logo_dir) else 'Directory not found'}")


def download_logo(brand, save_path):
    """Try to download logo using Clearbit API."""
    print(f"\n=== Searching logo for {brand} ===")

    # Clean brand name for domain search - handle special characters
    clean_name = brand.lower().replace("'", "").replace("'", "").replace(" ", "")
    print(f"Cleaned name: '{clean_name}'")

    # Special mappings for known brands
    brand_domains = {
        "southwest": "southwest.com",
        "autozone": "autozone.com",
        "ulta": "ulta.com",
        "grubhub": "grubhub.com",
        "wayfair": "wayfair.com",
        "klarna": "klarna.com",
        "kwiktrip": "kwiktrip.com",
        "krispykreme": "krispykreme.com",
        "jewelosco": "jewelosco.com",
        "niagara": "niagarawater.com",
        "niagarawater": "niagarawater.com"
    }

    # Get domain from mapping or construct it
    domain = brand_domains.get(clean_name)
    if not domain:
        # Try alternate domain patterns
        domains_to_try = [
            f"{clean_name}.com",
            f"www.{clean_name}.com",
            f"{clean_name}.net",
            f"{clean_name}.org"
        ]
    else:
        domains_to_try = [domain]

    print(f"Domains to try: {domains_to_try}")

    # Try each domain
    for test_domain in domains_to_try:
        try:
            url = f"https://logo.clearbit.com/{test_domain}"
            print(f"Trying URL: {url}")

            response = requests.get(url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })

            print(f"Response status: {response.status_code}")
            print(f"Response headers: {dict(response.headers)}")

            if response.status_code == 200:
                # Check if we actually got an image
                content_type = response.headers.get('content-type', '')
                print(f"Content type: {content_type}")

                if 'image' in content_type:
                    with open(save_path, "wb") as f:
                        f.write(response.content)
                    print(f"✓ Downloaded logo for {brand} from {test_domain}")
                    return True
                else:
                    print(f"✗ Got non-image response from {test_domain}")
        except Exception as e:
            print(f"✗ Error trying {test_domain}: {str(e)}")
            continue

    print(f"✗ Could not find logo for {brand}, creating placeholder")
    return False


def create_text_logo(brand, save_path):
    """Create a text-based logo for brands where download fails."""
    img = Image.new('RGBA', (400, 400), (255, 255, 255, 0))
    draw = ImageDraw.Draw(img)

    # Brand-specific colors
    brand_colors = {
        "AutoZone": (255, 0, 0),  # Red
        "Southwest": (0, 0, 139),  # Dark blue
    }

    # Get color or default to dark gray
    color = brand_colors.get(brand, (100, 100, 100))

    # Get initials or short name
    text = ''.join([word[0].upper() for word in brand.split()[:2]])

    # Calculate text size (approximate)
    font_size = 200 if len(text) == 1 else 150

    # Draw text centered
    # Using a large font size for visibility
    draw.text((200, 200), text, fill=color + (255,), anchor="mm")

    img.save(save_path)
    print(f"Created text logo for {brand}")


def generate_professional_wheel(csv_file="mock_fan_wheel.csv",
                                output_file="professional_fan_wheel.png",
                                center_text="THE SKY FAN",
                                team_color="#1D428A",
                                force_regenerate=True):  # Utah Jazz blue

    # Load or create data - REMOVED McDonald's and Binny's
    # Always create fresh data with 10 brands
    data = {
        'brand': ['AutoZone', 'Southwest', 'Klarna', 'Kwik Trip', 'Wayfair',
                  'GrubHub', 'Ulta', 'Krispy Kreme', 'Jewel-Osco',
                  'Niagara Water'],  # Removed McDonald's and Binny's
        'behavior': ['Shops at\nAutoZone', 'Flys with\nSouthwest', 'Pays with\nKlarna',
                     'Fills up at\nKwik Trip', 'Decorates with\nWayfair', 'Delivery with\nGrubHub',
                     'Beauty at\nUlta', 'Indulges at\nKrispy Kreme',
                     'Groceries at\nJewel-Osco', 'Drinks\nNiagara Water'],  # Removed corresponding behaviors
        'logo_path': [''] * 10  # Changed from 12 to 10
    }
    df = pd.DataFrame(data)
    df.to_csv(csv_file, index=False)
    print(f"✅ Created new CSV file with {len(df)} brands")

    # Load data - always print what we're working with
    df = pd.read_csv(csv_file)
    print("\n=== Loaded brand data ===")
    print(f"Total brands: {len(df)}")
    for idx, row in df.iterrows():
        print(f"Brand {idx}: '{row['brand']}' (length: {len(row['brand'])})")
    print("========================\n")

    # Download logos
    for idx, row in df.iterrows():
        brand = row['brand']
        # Generate filename - remove all apostrophes
        filename = brand.lower().replace(' ', '_').replace("'", '').replace("'", '') + ".png"
        filepath = os.path.join(logo_dir, filename)

        # Debug: print the exact path being checked
        print(f"Checking for {brand}: {filepath}")

        if not os.path.exists(filepath):
            print(f"  File not found at {filepath}")
            success = download_logo(brand, filepath)
            if not success:
                create_text_logo(brand, filepath)
        else:
            print(f"✓ Using existing logo for {brand} ({filename})")

        df.at[idx, 'logo_path'] = filepath

    # Create figure with equal aspect ratio
    fig = plt.figure(figsize=(12, 12), facecolor='white')
    ax = fig.add_subplot(111, aspect='equal')
    ax.set_xlim(-6, 6)
    ax.set_ylim(-6, 6)
    ax.axis('off')

    num_items = len(df)
    angle_step = 360 / num_items

    # Parameters
    outer_radius = 5.0
    logo_radius = 2.5  # Keep logos at original position
    middle_radius = logo_radius  # Move color boundary to match logo center
    inner_radius = 1.8
    text_radius = 3.85  # Moved further inward for better containment

    # Color variations for two-tone effect (INVERTED)
    outer_color = '#4169E1'  # Lighter blue on outer ring
    inner_color = team_color  # Darker blue on inner ring

    # First, draw all wedges without borders to avoid white lines between colors
    for i in range(num_items):
        start_angle = i * angle_step - 90
        end_angle = (i + 1) * angle_step - 90

        # Draw full wedge from outer to center (will be overlapped)
        full_wedge = Wedge((0, 0), outer_radius, start_angle, end_angle,
                           width=outer_radius,
                           facecolor=inner_color,  # Start with inner color
                           edgecolor='none',
                           zorder=1)
        ax.add_patch(full_wedge)

        # Overlay outer ring with darker color
        outer_ring = Wedge((0, 0), outer_radius, start_angle, end_angle,
                           width=outer_radius - middle_radius,
                           facecolor=outer_color,
                           edgecolor='none',
                           zorder=2)
        ax.add_patch(outer_ring)

    # Now add white dividing lines on top
    for i in range(num_items):
        angle = i * angle_step - 90
        angle_rad = np.deg2rad(angle)

        # Draw radial line from center to outer edge
        x_inner = inner_radius * np.cos(angle_rad)
        y_inner = inner_radius * np.sin(angle_rad)
        x_outer = outer_radius * np.cos(angle_rad)
        y_outer = outer_radius * np.sin(angle_rad)

        ax.plot([x_inner, x_outer], [y_inner, y_outer],
                color='white', linewidth=5, zorder=15)

    # Add arrows between logos and text (white circles with yellow arrows)
    for i in range(num_items):
        angle = i * angle_step - 90
        arrow_angle = np.deg2rad(angle)

        # Arrow position - keep at original position (3.2)
        arrow_r = 3.2  # Original middle_radius position
        arrow_x = arrow_r * np.cos(arrow_angle)
        arrow_y = arrow_r * np.sin(arrow_angle)

        # Add white circle background
        circle_bg = Circle((arrow_x, arrow_y), 0.175,  # Reduced by 50% from 0.35
                           facecolor='white',
                           edgecolor='none',
                           zorder=12)
        ax.add_patch(circle_bg)

        # Create yellow arrow pointing clockwise
        arrow_size = 0.1  # Reduced by 50% from 0.2
        arrow_direction = arrow_angle - np.pi / 2  # -90 degrees for clockwise

        # Arrow vertices forming a triangle
        tip_x = arrow_x + arrow_size * np.cos(arrow_direction)
        tip_y = arrow_y + arrow_size * np.sin(arrow_direction)

        base_center_x = arrow_x - arrow_size * 0.5 * np.cos(arrow_direction)
        base_center_y = arrow_y - arrow_size * 0.5 * np.sin(arrow_direction)

        # Base points perpendicular to arrow direction
        base_offset = arrow_size * 0.35
        base1_x = base_center_x + base_offset * np.cos(arrow_direction + np.pi / 2)
        base1_y = base_center_y + base_offset * np.sin(arrow_direction + np.pi / 2)
        base2_x = base_center_x - base_offset * np.cos(arrow_direction + np.pi / 2)
        base2_y = base_center_y - base_offset * np.sin(arrow_direction + np.pi / 2)

        arrow = Polygon([(tip_x, tip_y), (base1_x, base1_y), (base2_x, base2_y)],
                        facecolor='#FFD700', edgecolor='#FFD700', zorder=13)  # Gold arrow
        ax.add_patch(arrow)

    # Add center black circle with yellow border
    center_circle = Circle((0, 0), inner_radius,
                           facecolor='black',
                           edgecolor='#FFD700',  # Yellow border to match arrows
                           linewidth=5,
                           zorder=20)
    ax.add_patch(center_circle)

    # Add center text
    ax.text(0, 0, center_text,
            ha='center', va='center',
            fontsize=24, fontweight='bold',  # Increased from 20
            color='white', zorder=21)

    # Add logos and text inside wedges
    for i, row in df.iterrows():
        # Calculate center angle of wedge
        center_angle = i * angle_step + angle_step / 2 - 90
        angle_rad = np.deg2rad(center_angle)

        # Logo position (inner ring)
        logo_x = logo_radius * np.cos(angle_rad)
        logo_y = logo_radius * np.sin(angle_rad)

        # Add white circle background for logo
        logo_bg = Circle((logo_x, logo_y), 0.5,
                         facecolor='white',
                         edgecolor='none',
                         zorder=5)
        ax.add_patch(logo_bg)

        # Add logo
        try:
            # Process logo for transparency
            from PIL import Image as PILImage

            # Open and process the image
            pil_img = PILImage.open(row['logo_path'])
            if pil_img.mode != 'RGBA':
                pil_img = pil_img.convert('RGBA')

            # Convert PIL image to numpy array for matplotlib
            img_array = np.array(pil_img)

            # Use processed image with transparency
            imagebox = OffsetImage(img_array, zoom=0.35)
            ab = AnnotationBbox(imagebox, (logo_x, logo_y),
                                frameon=False, zorder=6)
            ax.add_artist(ab)
        except Exception as e:
            # If logo fails, add text initials
            initials = ''.join([word[0].upper() for word in row['brand'].split()[:2]])
            ax.text(logo_x, logo_y, initials,
                    ha='center', va='center',
                    fontsize=12, fontweight='bold',
                    color='gray', zorder=6)

        # Add behavior text (outer ring) - WHITE TEXT, CENTERED IN WEDGE
        # Calculate the middle of the outer ring section
        text_radius_center = (middle_radius + outer_radius) / 2
        text_x = text_radius_center * np.cos(angle_rad)
        text_y = text_radius_center * np.sin(angle_rad)

        # Force all text to be two lines for consistent formatting
        text = row['behavior'].replace('\n', ' ')  # Remove existing line breaks
        words = text.split()

        # Always split into two lines
        if len(words) >= 2:
            # Find the best split point
            if len(words) == 2:
                line1 = words[0]
                line2 = words[1]
            else:
                # Split as evenly as possible
                mid = len(words) // 2
                # Adjust split to avoid orphaned short words
                if len(words) > 3 and len(words[mid - 1]) <= 3:
                    mid -= 1
                line1 = ' '.join(words[:mid])
                line2 = ' '.join(words[mid:])
            formatted_text = f"{line1}\n{line2}"
        else:
            # Single word - just use as is
            formatted_text = text

        ax.text(text_x, text_y, formatted_text,
                ha='center', va='center',
                fontsize=14, fontweight='bold',  # Increased from 11 to 14
                color='white',  # WHITE TEXT
                rotation=0,  # No rotation - keep horizontal
                linespacing=0.8,  # Tighter line spacing
                zorder=7)

    # Save with high quality
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close()

    print(f"\n✅ Professional fan wheel saved as {output_file}")
    print(f"✅ Generated wheel with {num_items} brands")
    return output_file


# Generate the wheel
if __name__ == "__main__":
    generate_professional_wheel()