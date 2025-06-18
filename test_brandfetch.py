import os
import requests
import json
from PIL import Image
from io import BytesIO
from typing import Optional, Dict, List
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class BrandfetchAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.brandfetch.io/v2"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json"
        }

    def test_authentication(self) -> bool:
        """Test if API key is valid"""
        print("🔐 Testing API authentication...")

        # Try a simple request to verify auth
        test_domain = "google.com"
        url = f"{self.base_url}/brands/{test_domain}"

        try:
            response = requests.get(url, headers=self.headers, timeout=5)
            if response.status_code == 200:
                print("✅ API key is valid!")
                return True
            elif response.status_code == 401:
                print("❌ Invalid API key - Authentication failed")
                return False
            elif response.status_code == 403:
                print("❌ Forbidden - Check your API key permissions")
                return False
            else:
                print(f"❌ Unexpected status: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False

    def get_brand_info(self, domain: str) -> Dict:
        """Get complete brand information including logos"""

        print(f"\n🔍 Fetching brand info for: {domain}")

        # Clean domain (remove https://, www., etc)
        domain = domain.lower().replace("https://", "").replace("http://", "").replace("www.", "").strip("/")

        url = f"{self.base_url}/brands/{domain}"

        try:
            response = requests.get(url, headers=self.headers, timeout=10)

            print(f"   Status: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                print("   ✅ Brand data retrieved successfully!")

                # Print some info about what we found
                if "logos" in data:
                    print(f"   📸 Found {len(data['logos'])} logo variations")

                return data
            elif response.status_code == 404:
                print("   ❌ Brand not found")
                return None
            elif response.status_code == 401:
                print("   ❌ Authentication failed - check API key")
                return None
            elif response.status_code == 403:
                print("   ❌ Forbidden - check API key permissions")
                return None
            else:
                print(f"   ❌ Error: {response.status_code}")
                if response.text:
                    print(f"   Response: {response.text[:200]}")
                return None

        except Exception as e:
            print(f"   ❌ Request failed: {e}")
            return None

    def download_best_logo(self, domain: str, save_dir: str = "logos") -> Optional[str]:
        """Download the best available logo for a domain"""

        # Create directory if needed
        Path(save_dir).mkdir(exist_ok=True)

        # Get brand info
        brand_data = self.get_brand_info(domain)

        if not brand_data:
            return None

        # Extract logos
        logos = brand_data.get("logos", [])

        if not logos:
            print("   ❌ No logos found in brand data")
            return None

        # Find the best logo (prefer icon/symbol for fan wheel)
        best_logo = None

        # Priority order for logo types (icon/symbol work best for circular layouts)
        type_priority = ["icon", "symbol", "logo", "logomark", "wordmark"]

        for logo_type in type_priority:
            for logo in logos:
                if logo.get("type") == logo_type:
                    best_logo = logo
                    break
            if best_logo:
                break

        # If no preferred type found, use first one
        if not best_logo:
            best_logo = logos[0]

        print(f"   Selected logo type: {best_logo.get('type', 'unknown')}")

        # Get formats
        formats = best_logo.get("formats", [])

        if not formats:
            print("   ❌ No formats available")
            return None

        # Find best format (prefer PNG for compatibility)
        best_format = None

        for format_type in ["png", "jpg", "jpeg", "svg"]:
            for fmt in formats:
                if fmt.get("format") == format_type:
                    best_format = fmt
                    break
            if best_format:
                break

        if not best_format:
            best_format = formats[0]

        # Download the logo
        logo_url = best_format.get("src")

        if not logo_url:
            print("   ❌ No download URL found")
            return None

        print(f"   Downloading: {logo_url}")

        try:
            response = requests.get(logo_url, timeout=10)

            if response.status_code == 200:
                # Clean filename
                clean_domain = domain.replace(".", "_").replace("/", "_")
                format_type = best_format.get("format", "png")
                filename = f"{clean_domain}_{best_logo.get('type', 'logo')}.{format_type}"
                filepath = Path(save_dir) / filename

                # Save the file
                with open(filepath, 'wb') as f:
                    f.write(response.content)

                print(f"   ✅ Logo saved: {filepath}")

                # Show dimensions if it's an image
                if format_type in ["png", "jpg", "jpeg"]:
                    try:
                        img = Image.open(filepath)
                        print(f"   📐 Dimensions: {img.width}x{img.height}")
                    except:
                        pass

                return str(filepath)
            else:
                print(f"   ❌ Failed to download: {response.status_code}")
                return None

        except Exception as e:
            print(f"   ❌ Download error: {e}")
            return None


def test_brandfetch_with_companies():
    """Test Brandfetch API with your specific companies"""

    # Load API key from .env file
    API_KEY = os.getenv("BRANDFETCH_API_KEY")

    if not API_KEY:
        print("❌ BRANDFETCH_API_KEY not found in .env file!")
        print("\nMake sure your .env file contains:")
        print('BRANDFETCH_API_KEY="your-actual-api-key"')

        # Check if .env file exists
        if not Path(".env").exists():
            print("\n⚠️  No .env file found in current directory!")
            print(f"   Current directory: {Path.cwd()}")
        else:
            print("\n✅ .env file found")
            # Try to show what's in it (without showing the actual key)
            with open(".env", "r") as f:
                lines = f.readlines()
                for line in lines:
                    if "BRANDFETCH_API_KEY" in line:
                        print(f"   Found line: {line.split('=')[0]}=***")
        return

    print(f"✅ API key loaded from .env file")
    print(f"   Key preview: {API_KEY[:10]}...{API_KEY[-5:]}")

    # Initialize API client
    bf = BrandfetchAPI(API_KEY)

    # Test authentication first
    if not bf.test_authentication():
        print("\n❌ Authentication failed! Please check your API key.")
        return

    # Your test companies
    test_companies = [
        # Major brands (should work)
        "autozone.com",
        "southwest.com",
        "ulta.com",
        "wayfair.com",
        "grubhub.com",
        "krispykreme.com",

        # Challenging ones
        "fanfavorite.com",
        "wahooz.com",
        "blastmotion.com",
        "scoot.com",
        "ukathletics.com",  # Kentucky Athletics
        "cosmstudios.com",  # Cosm
        "elcapitantheatre.com",  # El Capitan Theatre
        "prana.com",
        "nfhs.com",
        "drewhouse.com"
    ]

    print("\n🚀 Starting logo downloads...")
    print("=" * 60)

    results = {}

    for domain in test_companies:
        filepath = bf.download_best_logo(domain)
        results[domain] = filepath
        print("-" * 60)

    # Summary
    print("\n📊 SUMMARY")
    print("=" * 60)

    success = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)

    print(f"✅ Success: {success}/{len(test_companies)}")
    print(f"❌ Failed: {failed}/{len(test_companies)}")

    print("\nSuccessful downloads:")
    for domain, path in results.items():
        if path:
            print(f"  ✅ {domain} → {path}")

    print("\nFailed downloads:")
    for domain, path in results.items():
        if not path:
            print(f"  ❌ {domain}")


if __name__ == "__main__":
    # Run the test
    test_brandfetch_with_companies()