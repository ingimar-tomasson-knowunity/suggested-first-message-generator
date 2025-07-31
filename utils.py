import csv
import os
from typing import List, Dict, Tuple
from model import Country, Subject, Grade, Language, Combination


def ensure_data_directory():
    """Ensure the ./data directory exists."""
    os.makedirs('./data', exist_ok=True)


def load_countries() -> List[Country]:
    """Load countries from CSV file."""
    countries: List[Country] = []
    with open('./data/country_table.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            countries.append(Country(id=int(row['id']), english_name=row['english_name']))
    return countries


def load_subjects() -> List[Subject]:
    """Load subjects from CSV file."""
    subjects: List[Subject] = []
    with open('./data/subject_table.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            subjects.append(Subject(
                id=int(row['id']),
                country_id=int(row['country_id']),
                long_name=row['long_name']
            ))
    return subjects


def load_grades() -> List[Grade]:
    """Load grades from CSV file."""
    grades: List[Grade] = []
    with open('./data/grade_table.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            grades.append(Grade(
                id=int(row['id']),
                country_id=int(row['country_id']),
                long_name=row['long_name']
            ))
    return grades


def load_languages() -> List[Language]:
    """Load languages from CSV file."""
    languages: List[Language] = []
    with open('./data/language_table.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            languages.append(Language(id=int(row['id']), english_name=row['english_name']))
    return languages


def create_combinations():
    """Create combination files for each country."""
    ensure_data_directory()

    countries = load_countries()
    subjects = load_subjects()
    grades = load_grades()

    for country in countries:
        country_subjects = [s for s in subjects if s.country_id == country.id]
        country_grades = [g for g in grades if g.country_id == country.id]

        combinations: List[Combination] = []
        for grade in country_grades:
            for subject in country_subjects:
                combinations.append(Combination(
                    country_id=country.id,
                    grade_id=grade.id,
                    subject_id=subject.id
                ))

        filename = f'./data/combos_{country.id}.csv'
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['country_id', 'grade_id', 'subject_id'])
            for combo in combinations:
                writer.writerow([combo.country_id, combo.grade_id, combo.subject_id])


def get_names_for_combination(country_id: int, grade_id: int, subject_id: int) -> Tuple[str, str, str]:
    """Get country, grade, and subject names for a given combination."""
    countries = load_countries()
    subjects = load_subjects()
    grades = load_grades()

    country_name = next((c.english_name for c in countries if c.id == country_id), "Unknown")
    subject_name = next((s.long_name for s in subjects if s.id == subject_id), "Unknown")
    grade_name = next((g.long_name for g in grades if g.id == grade_id), "Unknown")

    return country_name, grade_name, subject_name


def load_combinations_for_country(country_id: int) -> List[Combination]:
    """Load combinations for a specific country."""
    combinations: List[Combination] = []
    filename = f'./data/combos_{country_id}.csv'

    if not os.path.exists(filename):
        return combinations

    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            combinations.append(Combination(
                country_id=int(row['country_id']),
                grade_id=int(row['grade_id']),
                subject_id=int(row['subject_id'])
            ))

    return combinations


def save_csv_data(filename: str, data: List[Dict], fieldnames: List[str]):
    """Save data to CSV file."""
    ensure_data_directory()
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


# ---------------------------------------------------------------------------
# Helper lookup utilities
# ---------------------------------------------------------------------------


def get_country_id_by_name(country_name: str) -> int:
    """Return the country_id for the given English country name (case-insensitive)."""
    for country in load_countries():
        if country.english_name.lower() == country_name.lower():
            return country.id
    raise ValueError(f"Country '{country_name}' not found.")


def get_language_id_by_name(language_name: str) -> int:
    """Return the language_id for the given English language name (case-insensitive)."""
    for language in load_languages():
        if language.english_name.lower() == language_name.lower():
            return language.id
    raise ValueError(f"Language '{language_name}' not found.")


def get_languages_for_country(country_name: str, top_n: int = 2) -> List[str]:
    """Return the most likely languages spoken in a country.

    This is a simple placeholder based on a small hard-coded mapping. When a
    country is not present in the mapping, the function falls back to the first
    `top_n` languages defined in the language table (usually English).
    """
    mapping: Dict[str, List[str]] = {
        "germany": ["German", "English"],
        "united states": ["English", "Spanish"],
        "france": ["French", "English"],
        "spain": ["Spanish", "English"],
        "italy": ["Italian", "English"],
        "brazil": ["Portuguese", "English"],
        "india": ["Hindi", "English"],
    }

    langs = mapping.get(country_name.lower())
    if langs:
        return langs[:top_n]

    # Fallback – just return the first N languages from the language table
    return [lang.english_name for lang in load_languages()[:top_n]]
