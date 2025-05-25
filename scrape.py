import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from bs4 import BeautifulSoup

BASE_URL = "https://www.mulinobianco.it"
LISTING_URL = f"{BASE_URL}/prodotti/biscotti-e-dolcetti"
MAX_CONCURRENT_PAGES = 6

async def extract_nutrition_table(page):
    try:
        await page.wait_for_selector("#nutritionalValues table", timeout=8000)
        table_handle = await page.query_selector("#nutritionalValues table")
        html = await table_handle.inner_html()
        soup = BeautifulSoup(html, "html.parser")

        rows = []
        for tr in soup.select("tr"):
            cols = [td.get_text(separator=" ", strip=True).replace('\xa0', ' ') for td in tr.find_all("td")]
            if cols:
                rows.append(cols)

        return rows

    except Exception as e:
        print(f"Error extracting nutrition table: {e}")
        return []

async def extract_ingredients(page):
    try:
        await page.wait_for_selector("div.ingredients-box div.text-cnt", timeout=8000)

        # Locate the CON: ingredients section
        sections = await page.query_selector_all("div.ingredients-box div.text-cnt h5.mb-blue-title")
        for h in sections:
            title = (await h.inner_text()).strip().upper()
            if title.startswith("CON"):
                sibling = await h.evaluate_handle("el => el.nextElementSibling")
                html_content = await sibling.inner_html()
                soup = BeautifulSoup(html_content, "html.parser")

                for a in soup.find_all("a"):
                    a.replace_with(a.get_text(strip=True, separator=" "))

                return soup.get_text(separator=" ", strip=True)

        return "N/A"

    except Exception as e:
        print(f"Error extracting ingredients: {e}")
        return "N/A"

async def scrape_product_details(context, product_data, semaphore):
    async with semaphore:
        page = await context.new_page()
        url = product_data["biscuit_url"]
        await page.goto(url, timeout=30000)
        ingredients = await extract_ingredients(page)
        nutrition = await extract_nutrition_table(page)
        await page.close()
        product_data["ingredients"] = ingredients
        product_data["nutrition"] = nutrition
        return product_data

async def scrape_all():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        )

        page = await context.new_page()
        await page.goto(LISTING_URL, timeout=60000)

        try:
            await page.click("#onetrust-reject-all-handler", timeout=7000)
            print("Cookie banner declined.")
        except:
            print("Cookie decline button not found or already handled.")

        await page.wait_for_selector('div[mb-component="ProductListComponent"] div.thumbnail[data-type="PRODUCT"]', timeout=15000)
        products = await page.query_selector_all('div[mb-component="ProductListComponent"] div.thumbnail[data-type="PRODUCT"]')
        print(f"Found {len(products)} products.")

        data = []

        for idx, product in enumerate(products, start=1):
            try:
                thumbnail_product = await product.query_selector('.thumbnail-product')
                if not thumbnail_product:
                    print(f"No .thumbnail-product inside product #{idx}")
                    continue

                name_el = await thumbnail_product.query_selector('.thumbnail__image__text .inner-text')
                thumbnail_img = await thumbnail_product.query_selector('.thumbnail__image img')
                page_link_el = await thumbnail_product.query_selector('.thumbnail__image__widelink')

                name = await name_el.inner_text() if name_el else "N/A"
                image_url = await thumbnail_img.get_attribute('data-src') if thumbnail_img else "N/A"
                biscuit_url = await page_link_el.get_attribute('href') if page_link_el else "N/A"

                if image_url.startswith("/"):
                    image_url = BASE_URL + image_url
                if biscuit_url.startswith("/"):
                    biscuit_url = BASE_URL + biscuit_url

                data.append({"name": name.strip(), "image_url": image_url, "biscuit_url": biscuit_url})

            except Exception as e:
                print(f"Error scraping product #{idx}: {e}")

        await page.close()

        print("Extracting ingredients from product pages...")
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
        tasks = [scrape_product_details(context, prod, semaphore) for prod in data]
        all_data = await asyncio.gather(*tasks)

        await browser.close()

        df = pd.DataFrame(all_data)
        df.to_csv("scraped\mulino_biscuits.csv", index=False)
        print(f"Scraped {len(df)} products and saved to mulino_biscuits.csv")


if __name__ == "__main__":
    asyncio.run(scrape_all())