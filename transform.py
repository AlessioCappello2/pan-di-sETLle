import pandas as pd
import google.generativeai as genai # pip install google-generativeai
import ast
import time
import json
import re
from collections import Counter
from tenacity import retry, stop_after_attempt, wait_fixed
from concurrent.futures import ThreadPoolExecutor

GEMINI_API_KEY = "apikey"
PROMPT_FILE = "prompt.txt"
INGREDIENTS_FILE = "scraped\ingredients.txt"

def ingredients_processing(df: pd.DataFrame):
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def safe_llm_call(curr_ingredients):
        response = model.generate_content(prompt + curr_ingredients)
        try:
            parsed = ast.literal_eval(response.text)
            return parsed
        except Exception:
            raise

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("models/gemini-1.5-flash")

    with open(PROMPT_FILE, "r") as f:
        prompt = f.read()

    with open(INGREDIENTS_FILE, "r", encoding='utf-8') as f:
        old_ingredients = ast.literal_eval(f.read())

    ingredients_cleaned = []
    counter = Counter()
    for i in range(len(df)):
        try:
            parsed = safe_llm_call(df.iloc[i]["ingredients"])
            ingredients_cleaned.append(parsed)
            counter.update(parsed)
            print(parsed)
        except Exception:
            print(f"Failed to get ingredients while processing record #{i}, using last month's ingredients.")
            ingredients_cleaned.append(old_ingredients[i])
            counter.update(old_ingredients[i])
        
        time.sleep(5)

    with open("scraped\ingredients_freq.json", "w", encoding='utf-8') as f:
        json.dump(counter, f, ensure_ascii=False, indent=2)


def nutrition_processing(df: pd.DataFrame):
    nutrition_rows = []
    headers = {
        "ENERGIA": "energia (g)",
        "FIBRE": "fibre (g)",
        "PROTEINE": "proteine (g)",
        "SALE": "sale (g)"
    }

    for i in range(len(df)):
        record = {"name": df.iloc[i]["name"]}
        row = ast.literal_eval(df.iloc[i]["nutrition"])
        record["energia (kcal)"] = int(re.sub(r"\D", "", row[2][1].split(" ")[2]))
        for j in range(3, len(row)): 
            if row[j][0] in headers:
                record[headers[row[j][0]]] = float(re.sub(r"[^0-9.]", "", row[j][1].split(" g")[0].replace(",", ".")))
            elif row[j][0].startswith("CARBOIDRATI"):
                record["carboidrati (g)"] = float(re.sub(r"[^0-9.]", "", row[j][1].split(" g")[0].replace(",", ".")))
            elif row[j][0].startswith("GRASSI"):
                record["grassi (g)"] = float(re.sub(r"[^0-9.]", "", row[j][1].split(" g")[0].replace(",", ".")))
        nutrition_rows.append(record)

    nutrition_df = pd.DataFrame(nutrition_rows)
    nutrition_df.to_csv("scraped\\nutrition.csv", index=False)

if __name__ == "__main__":
    df = pd.read_csv("scraped\mulino_biscuits.csv")

    with ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(ingredients_processing, df)
        future2 = executor.submit(nutrition_processing, df)

        _, _ = future1.result(), future2.result()