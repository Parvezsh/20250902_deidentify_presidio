# pip install presidio-analyzer presidio-anonymizer pandas tqdm concurrent.futures

import json
import pandas as pd
from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern
from presidio_anonymizer import AnonymizerEngine
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

class AgeRecognizer(PatternRecognizer):
    """
    Recognizes age expressions in medical text, including multiple common formats.
    """
    def __init__(self):
        patterns = [
            Pattern(name="age_dash_year_old", regex=r"\b\d{1,3}\s?-?\s?year-?old\b", score=0.95),
            Pattern(name="age_dash_yrs_old", regex=r"\b\d{1,3}\s?-?\s?yrs?-?old\b", score=0.95),
            Pattern(name="age_dash_yr_old", regex=r"\b\d{1,3}\s?-?\s?yr-?old\b", score=0.95),
            Pattern(name="age_yo", regex=r"\b\d{1,3}\s?yo\b", score=0.95),
            Pattern(name="age_y_o", regex=r"\b\d{1,3}\s?y/o\b", score=0.95),
            Pattern(name="age_colon", regex=r"\bage[:\s]\s?\d{1,3}\b", score=0.95),
            Pattern(name="age_years", regex=r"\b\d{1,3}\s?years?\b", score=0.9),
            Pattern(name="age_yrs", regex=r"\b\d{1,3}\s?yrs?\b", score=0.9),
            Pattern(name="age_yr", regex=r"\b\d{1,3}\s?yr\b", score=0.9),
            Pattern(name="age_parenthesis", regex=r"\( ?\d{1,3} ?\)", score=0.85)
        ]
        super().__init__(supported_entity="AGE", patterns=patterns)


# ---------- Initialize Presidio ----------
analyzer = AnalyzerEngine()
analyzer.registry.add_recognizer(AgeRecognizer())
anonymizer = AnonymizerEngine()

# ---------- Input/Output ----------
INPUT_FILE = "/Users/phameed/Downloads/CCAD_Zabair_chart_notes.json"   # JSON file
OUTPUT_FILE = "/Users/phameed/Downloads/CCAD_Zabair_chart_notes_deidentified.xlsx"
ENTITIES = ['PERSON', 'EMAIL_ADDRESS', 'PHONE_NUMBER', 'LOCATION', 'DATE_TIME', 'AGE']

# ---------- De-identification ----------
def deidentify_text(text):
    try:
        if not isinstance(text, str) or text.strip() == "":
            return text
        results = analyzer.analyze(text=text, language='en', entities=ENTITIES)
        anonymized = anonymizer.anonymize(text=text, analyzer_results=results)
        return anonymized.text
    except Exception as e:
        print(f"Error processing text: {e}")
        return text

def process_row(row):
    # Add de-identified transcription
    row["transcription_DeID"] = deidentify_text(row.get("transcription", ""))

    # Email
    row["email"] = row.get("createdBy")

    # Domain (take last part after @ if email, else whole string)
    created_by = row.get("createdBy", "")
    if "@" in created_by:
        row["domain"] = created_by.split("@")[-1]
    else:
        row["domain"] = created_by  # fallback if no @

    # Created date
    created_at = row.get("createdAt", {})
    if isinstance(created_at, dict) and "$date" in created_at:
        row["created_date"] = created_at["$date"]
    else:
        row["created_date"] = None

    # Word count from transcription text
    text = row.get("transcription", "")
    if isinstance(text, str):
        row["word_count"] = len(text.split())
    else:
        row["word_count"] = 0

    return row


# ---------- Main ----------
def main():
    # Load JSON file (expects array of objects)
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)

    # Convert to DataFrame (keep selected fields)
    df = pd.DataFrame(data)

    records = df.to_dict(orient="records")

    processed_rows = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_row, row) for row in records]
        for f in tqdm(as_completed(futures), total=len(futures), desc="Processing"):
            processed_rows.append(f.result())

    # Final save
    pd.DataFrame(processed_rows).to_excel(OUTPUT_FILE, index=False)
    print(f"De-identified data saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
