from collections import defaultdict
from typing import List

import requests
from bs4 import BeautifulSoup

from utils import duration, write_to_file

QUOTES_API_URL = "https://breaking-bad-quotes.herokuapp.com/v1/quotes"

BREAKING_BAD_API_URL = "https://www.breakingbadapi.com/api"

BREAKING_BAD_WIKIPEDIA_URL = "https://breakingbad.fandom.com/wiki"


def get_breaking_bad_quotes_per_character(num_quotes: int) -> defaultdict:
    quotes = defaultdict(list)
    response = requests.get(f"{QUOTES_API_URL}/{num_quotes}")
    for quote in response.json():
        quotes[quote["author"]].append(quote["quote"])
    return quotes


def get_breaking_bad_characters() -> List:
    response = requests.get(f"{BREAKING_BAD_API_URL}/characters")
    if response.status_code == 200:
        return [character["name"] for character in response.json()]
    return []


def get_breaking_bad_wikipedia_character_info(character: str) -> str:
    url = f"{BREAKING_BAD_WIKIPEDIA_URL}/{character}"

    response = requests.get(url)

    soup = BeautifulSoup(response.text, features="html.parser")

    paragraphs = soup.select("p")
    return "\n".join([paragraph.text for paragraph in paragraphs[:5]])


@duration
def get_breaking_bad_characters_summary_and_write_to_file() -> None:
    # Takes ~20 seconds
    characters = get_breaking_bad_characters()
    for character in characters:
        wiki_summary = get_breaking_bad_wikipedia_character_info(character)
        write_to_file(
            file_name=character, file_content=f"{character}\nSummary:\n{wiki_summary}"
        )


if __name__ == "__main__":
    get_breaking_bad_characters_summary_and_write_to_file()
