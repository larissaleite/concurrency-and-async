from collections import defaultdict
from typing import Dict, List, Optional
from math import floor
from multiprocessing import cpu_count
import concurrent.futures
import requests
from bs4 import BeautifulSoup
import asyncio
from httpx import AsyncClient
from utils import duration, duration_async, write_to_file_async

from io_bound import BREAKING_BAD_API_URL, BREAKING_BAD_WIKIPEDIA_URL


async def get_breaking_bad_characters() -> List[str]:
    async with AsyncClient() as client:
        response = await client.get(f"{BREAKING_BAD_API_URL}/characters")
    if response.status_code == 200:
        return [character["name"] for character in response.json()]
    return []


async def get_breaking_bad_wikipedia_character_info(character: str) -> str:
    url = f"{BREAKING_BAD_WIKIPEDIA_URL}/{character}"

    async with AsyncClient() as client:
        response = await client.get(url)

    soup = BeautifulSoup(response.text, features="html.parser")

    paragraphs = soup.select("p")
    return "\n".join([paragraph.text for paragraph in paragraphs[:5]])


async def get_breaking_bad_characters_summary_and_write_to_file(
    characters: Optional[List] = None
) -> None:
    if not characters:
        characters = await get_breaking_bad_characters()

    for character in characters:
        wiki_summary = await get_breaking_bad_wikipedia_character_info(character)
        await write_to_file_async(
            file_name=character, file_content=f"{character}\nSummary:\n{wiki_summary}"
        )


@duration_async
async def get_breaking_bad_characters_summary_and_write_to_file() -> None:
    # Takes ~20 seconds
    await get_breaking_bad_characters_summary_and_write_to_file()


def get_breaking_bad_characters_summary_and_write_to_file_wrapper(
    characters: List
) -> None:
    asyncio.run(get_breaking_bad_characters_summary_and_write_to_file(characters))


@duration
def get_breaking_bad_characters_summary_and_write_to_file_multiprocessing() -> None:
    # Takes ~10 seconds
    characters = asyncio.run(get_breaking_bad_characters())

    NUM_PAGES = len(characters)
    NUM_CORES = cpu_count()

    PAGES_PER_CORE = floor(NUM_PAGES / NUM_CORES)
    PAGES_FOR_FINAL_CORE = PAGES_PER_CORE + NUM_PAGES % PAGES_PER_CORE

    futures = []

    with concurrent.futures.ProcessPoolExecutor(NUM_CORES) as executor:
        for i in range(NUM_CORES - 1):
            futures.append(
                executor.submit(
                    get_breaking_bad_characters_summary_and_write_to_file_wrapper,
                    characters=characters[
                        i * PAGES_PER_CORE : (i + 1) * PAGES_PER_CORE
                    ],
                )
            )

        futures.append(
            executor.submit(
                get_breaking_bad_characters_summary_and_write_to_file_wrapper,
                characters[(i + 1) * PAGES_PER_CORE :],
            )
        )

    concurrent.futures.wait(futures)


if __name__ == "__main__":
    asyncio.run(get_breaking_bad_characters_summary_and_write_to_file())
    get_breaking_bad_characters_summary_and_write_to_file_multiprocessing()
