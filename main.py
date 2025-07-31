
import click
import csv
from bigquery import fetch_countries, fetch_subjects, fetch_grades, fetch_languages
from utils import (
    save_csv_data,
    create_combinations,
    get_names_for_combination,
    load_combinations_for_country,
    get_languages_for_country,
    get_country_id_by_name,
    get_language_id_by_name,
)
from gemini import generate_topics, generate_suggested_prompts
from postgresql import upload_to_postgresql
from model import SuggestedFirstMessage
from uuid import uuid4
from datetime import datetime


@click.group()
def cli():
    """A CLI to manage the first prompt macher workflow."""
    pass


@cli.command()
def download_data():
    """Downloads data from BigQuery and saves it to CSV files."""
    click.echo("Downloading data from BigQuery...")

    countries = fetch_countries()
    save_csv_data(
        "./data/country_table.csv",
        [{"id": r[0], "english_name": r[1]} for r in countries],
        ["id", "english_name"],
    )
    click.echo("Downloaded country data.")

    subjects = fetch_subjects()
    save_csv_data(
        "./data/subject_table.csv",
        [{"id": r[0], "country_id": r[1], "long_name": r[2]} for r in subjects],
        ["id", "country_id", "long_name"],
    )
    click.echo("Downloaded subject data.")

    grades = fetch_grades()
    save_csv_data(
        "./data/grade_table.csv",
        [{"id": r[0], "country_id": r[1], "long_name": r[2]} for r in grades],
        ["id", "country_id", "long_name"],
    )
    click.echo("Downloaded grade data.")

    languages = fetch_languages()
    save_csv_data(
        "./data/language_table.csv",
        [{"id": r[0], "english_name": r[1]} for r in languages],
        ["id", "english_name"],
    )
    click.echo("Downloaded language data.")

    click.echo("All data downloaded successfully.")


@cli.command()
def make_combinations():
    """Creates combination files for each country."""
    click.echo("Creating combination files...")
    create_combinations()
    click.echo("Combination files created successfully.")


@cli.command()
@click.option("--country", required=True, help="The country to generate topics for.")
def generate_all_topics(country: str):
    """Generates topics for a given country."""
    click.echo(f"Generating topics for {country}...")
    country_id = get_country_id_by_name(country)
    combinations = load_combinations_for_country(country_id)

    all_topics = []
    for combo in combinations:
        country_name, grade_name, subject_name = get_names_for_combination(
            combo.country_id, combo.grade_id, combo.subject_id
        )
        topics = generate_topics(subject_name, grade_name, country_name)
        for topic in topics:
            all_topics.append(
                {
                    "country_id": combo.country_id,
                    "grade_id": combo.grade_id,
                    "subject_id": combo.subject_id,
                    "topic": topic,
                }
            )

    save_csv_data(
        f"./data/topics_{country_id}.csv",
        all_topics,
        ["country_id", "grade_id", "subject_id", "topic"],
    )
    click.echo(f"Topics for {country} generated and saved.")


@cli.command()
@click.option("--country", required=True, help="The country to generate prompts for.")
@click.option("--upload", is_flag=True, help="Upload generated prompts to PostgreSQL.")
def generate_all_prompts(country: str, upload: bool):
    """Generates prompts for a given country and optionally uploads them."""
    click.echo(f"Generating prompts for {country}...")
    country_id = get_country_id_by_name(country)
    languages = get_languages_for_country(country)

    with open(f"./data/topics_{country_id}.csv", "r") as f:
        reader = csv.DictReader(f)
        topics_data = list(reader)

    all_prompts = []
    for row in topics_data:
        country_name, grade_name, subject_name = get_names_for_combination(
            int(row["country_id"]), int(row["grade_id"]), int(row["subject_id"])
        )
        for lang in languages:
            prompts = generate_suggested_prompts(
                row["topic"], country_name, grade_name, lang
            )
            language_id = get_language_id_by_name(lang)
            for prompt in prompts:
                message = SuggestedFirstMessage(
                    uuid=uuid4(),
                    created_on=datetime.utcnow(),
                    message=prompt,
                    language_id=language_id,
                    country_id=int(row["country_id"]),
                    grade_id=int(row["grade_id"]),
                    subject_id=int(row["subject_id"]),
                )
                all_prompts.append(message)

    if upload:
        click.echo("Uploading prompts to PostgreSQL...")
        upload_to_postgresql(all_prompts)
        click.echo("Prompts uploaded successfully.")
    else:
        click.echo("Skipping upload. Prompts are not saved.")


if __name__ == "__main__":
    cli()
