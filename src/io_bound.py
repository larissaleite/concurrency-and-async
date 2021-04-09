import concurrent.futures
from collections import defaultdict
from math import floor
from multiprocessing import cpu_count
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from utils import duration, timer, write_to_file

QUOTES_API_URL = "https://breaking-bad-quotes.herokuapp.com/v1/quotes"

BREAKING_BAD_API_URL = "https://www.breakingbadapi.com/api"

BREAKING_BAD_WIKIPEDIA_URL = "https://breakingbad.fandom.com/wiki"


def get_breaking_bad_quotes_per_character(num_quotes: int) -> defaultdict:
    quotes = defaultdict(list)
    response = requests.get(f"{QUOTES_API_URL}/{num_quotes}")
    for quote in response.json():
        quotes[quote["author"]].append(quote["quote"])
    return quotes


def get_breaking_bad_characters() -> List[str]:
    response = requests.get(f"{BREAKING_BAD_API_URL}/characters")
    if response.status_code == 200:
        return [character["name"] for character in response.json()]
    return []


def get_breaking_bad_random_character() -> Optional[str]:
    response = requests.get(f"{BREAKING_BAD_API_URL}/character/random")
    if response.status_code == 200:
        return response.json()
    return None


def get_breaking_bad_wikipedia_character_info(character: str) -> str:
    url = f"{BREAKING_BAD_WIKIPEDIA_URL}/{character}"

    response = requests.get(url)

    soup = BeautifulSoup(response.text, features="html.parser")

    paragraphs = soup.select("p")
    return "\n".join([paragraph.text for paragraph in paragraphs[:5]])


def get_breaking_bad_characters_summary_and_write_to_file(
    characters: Optional[List] = None
) -> None:
    if not characters:
        characters = get_breaking_bad_characters()

    for character in characters:
        wiki_summary = get_breaking_bad_wikipedia_character_info(character)
        write_to_file(
            file_name=character, file_content=f"{character}\nSummary:\n{wiki_summary}"
        )


@duration
def get_breaking_bad_characters_summary_and_write_to_file_sequential() -> None:
    # Takes ~23 seconds
    get_breaking_bad_characters_summary_and_write_to_file()


@duration
def get_breaking_bad_characters_summary_and_write_to_file_multiprocessing() -> None:
    # Takes ~12 seconds
    characters = get_breaking_bad_characters()

    TOTAL_CHARACTERS = len(characters)
    NUM_CORES = cpu_count()

    CHARS_PER_CORE = floor(TOTAL_CHARACTERS / NUM_CORES)

    futures = []

    with concurrent.futures.ProcessPoolExecutor(NUM_CORES) as executor:
        for i in range(NUM_CORES - 1):
            futures.append(
                executor.submit(
                    get_breaking_bad_characters_summary_and_write_to_file,
                    characters=characters[
                        i * CHARS_PER_CORE : (i + 1) * CHARS_PER_CORE
                    ],
                )
            )

        futures.append(
            executor.submit(
                get_breaking_bad_characters_summary_and_write_to_file,
                characters[(i + 1) * CHARS_PER_CORE :],
            )
        )

    concurrent.futures.wait(futures)


def get_breaking_bad_random_characters_N_times(n: int) -> None:
    # Takes ~205 seconds
    for i in range(n):
        get_breaking_bad_random_character()


@duration
def get_breaking_bad_random_characters_N_times_multiprocessing(n: int) -> None:
    # Takes ~51 seconds
    NUM_CORES = cpu_count()

    CALLS_PER_CORE = floor(n / NUM_CORES)
    CALLS_FOR_FINAL_CORE = CALLS_PER_CORE + n % CALLS_PER_CORE

    futures = []

    with concurrent.futures.ProcessPoolExecutor(NUM_CORES) as executor:
        for i in range(NUM_CORES):
            calls = CALLS_PER_CORE if i < NUM_CORES else CALLS_FOR_FINAL_CORE

            futures.append(
                executor.submit(get_breaking_bad_random_characters_N_times, calls)
            )

    concurrent.futures.wait(futures)


if __name__ == "__main__":
    get_breaking_bad_characters_summary_and_write_to_file_sequential()
    get_breaking_bad_characters_summary_and_write_to_file_multiprocessing()

    with timer("get_breaking_bad_random_characters_N_times_sequential"):
        get_breaking_bad_random_characters_N_times(250)
    get_breaking_bad_random_characters_N_times_multiprocessing(250)
