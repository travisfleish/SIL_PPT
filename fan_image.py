"""
Simple script to generate a photorealistic Utah Jazz fan image
"""

import os
import requests
from openai import OpenAI
from PIL import Image
from io import BytesIO
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def generate_jazz_fan_image(api_key=None):
    """Generate a photorealistic image of a Utah Jazz fan."""

    # Get API key from .env file
    if not api_key:
        api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        print("❌ Error: No OpenAI API key found")
        print("Create a .env file with: OPENAI_API_KEY=sk-...")
        return None

    # Initialize OpenAI client
    client = OpenAI(api_key=api_key)

    # Simple, direct prompt for photorealistic Jazz fan
    prompt = """Photorealistic photograph of Utah Jazz basketball fans in the crowd during a game. 
    Multiple fans of different ages wearing Utah Jazz jerseys (orange #45 jersey, yellow Jazz shirts, navy blue gear). 
    Authentic crowd atmosphere with fans cheering, some standing with arms raised, others sitting and watching intently. 
    Mix of men and women, different ages including families with children. 
    Natural arena lighting, candid crowd shot showing genuine reactions and emotions. 
    Background shows more fans in stadium seating. 
    Documentary photography style, like a real NBA game crowd photo."""

    print("🎨 Generating Jazz fan image...")

    try:
        # Generate image using DALL-E 3
        response = client.images.generate(
            model="dall-e-3",
            prompt=prompt,
            n=1,
            size="1024x1024",
            quality="hd",  # High quality for photorealism
            style="natural"  # Natural style for photorealism
        )

        image_url = response.data[0].url
        print(f"✅ Image generated successfully!")

        # Download the image
        img_response = requests.get(image_url)
        if img_response.status_code == 200:
            # Save the image
            img = Image.open(BytesIO(img_response.content))

            output_path = "jazz_fan_photorealistic.png"
            img.save(output_path)

            print(f"✅ Image saved as: {output_path}")
            print(f"📏 Image size: {img.size}")

            return output_path
        else:
            print("❌ Failed to download image")
            return None

    except Exception as e:
        print(f"❌ Error: {str(e)}")

        # Try with DALL-E 2 as fallback
        print("🔄 Trying with DALL-E 2...")
        try:
            response = client.images.generate(
                model="dall-e-2",
                prompt=prompt,
                n=1,
                size="1024x1024"
            )

            image_url = response.data[0].url

            # Download the image
            img_response = requests.get(image_url)
            if img_response.status_code == 200:
                img = Image.open(BytesIO(img_response.content))
                output_path = "jazz_fan_photorealistic.png"
                img.save(output_path)

                print(f"✅ Image saved as: {output_path}")
                return output_path

        except Exception as e2:
            print(f"❌ Fallback also failed: {str(e2)}")

    return None


if __name__ == "__main__":
    # Generate the image
    result = generate_jazz_fan_image()

    if result:
        print(f"\n🎉 Success! Your Jazz fan image is ready: {result}")
    else:
        print("\n😞 Failed to generate image. Please check your .env file.")