import requests

# Test various McDonald's domains
test_domains = [
    "mcdonalds.com",
    "www.mcdonalds.com",
    "corporate.mcdonalds.com",
    "mcdonalds.co.uk",
    "mcdonalds.ca"
]

print("Testing Clearbit Logo API for McDonald's...\n")

for domain in test_domains:
    url = f"https://logo.clearbit.com/{domain}"
    print(f"Testing: {url}")

    try:
        response = requests.get(url, timeout=5, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        print(f"  Status: {response.status_code}")
        print(f"  Content-Type: {response.headers.get('content-type', 'N/A')}")
        print(f"  Content-Length: {response.headers.get('content-length', 'N/A')}")

        if response.status_code == 200:
            # Save to test file
            with open(f"test_{domain.replace('.', '_')}.png", "wb") as f:
                f.write(response.content)
            print(f"  ✓ Saved as test_{domain.replace('.', '_')}.png")

    except Exception as e:
        print(f"  ✗ Error: {str(e)}")

    print()

# Also test the direct McDonald's logo URL
print("\nTesting direct McDonald's media URLs...")
direct_urls = [
    "https://www.mcdonalds.com/content/dam/sites/usa/nfl/icons/arches-logo_108x108.jpg",
    "https://upload.wikimedia.org/wikipedia/commons/thumb/3/36/McDonald%27s_Golden_Arches.svg/1200px-McDonald%27s_Golden_Arches.svg.png"
]

for url in direct_urls:
    print(f"Testing: {url[:50]}...")
    try:
        response = requests.get(url, timeout=5)
        print(f"  Status: {response.status_code}")
    except Exception as e:
        print(f"  Error: {str(e)}")