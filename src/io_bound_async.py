import asyncio
import concurrent.futures
from collections import defaultdict
from math import floor
from multiprocessing import cpu_count
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from httpx import AsyncClient

from io_bound import BREAKING_BAD_API_URL, BREAKING_BAD_WIKIPEDIA_URL
from utils import duration, duration_async, timer, write_to_file_async


async def get_breaking_bad_characters(
    client: Optional[AsyncClient] = None
) -> List[str]:
    if client is not None:
        response = await client.get(f"{BREAKING_BAD_API_URL}/characters")
    else:
        async with AsyncClient() as client:
            response = await client.get(f"{BREAKING_BAD_API_URL}/characters")
    if response.status_code == 200:
        return [character["name"] for character in response.json()]
    return []


async def get_breaking_bad_random_character(client: AsyncClient) -> Optional[str]:
    response = await client.get(f"{BREAKING_BAD_API_URL}/character/random")
    if response.status_code == 200:
        return response.json()
    return None


async def get_breaking_bad_wikipedia_character_info(
    character: str, client: AsyncClient
) -> str:
    url = f"{BREAKING_BAD_WIKIPEDIA_URL}/{character}"

    response = await client.get(url)

    soup = BeautifulSoup(response.text, features="html.parser")

    paragraphs = soup.select("p")
    return "\n".join([paragraph.text for paragraph in paragraphs[:5]])


async def get_breaking_bad_characters_summary_and_write_to_file(
    characters: Optional[List] = None
) -> None:
    async with AsyncClient() as client:
        if not characters:
            characters = await get_breaking_bad_characters(client)

        for character in characters:
            wiki_summary = await get_breaking_bad_wikipedia_character_info(
                character, client
            )
            await write_to_file_async(
                file_name=character,
                file_content=f"{character}\nSummary:\n{wiki_summary}",
            )


@duration_async
async def get_breaking_bad_characters_summary_and_write_to_file_sequential() -> None:
    # Takes ~20 seconds
    await get_breaking_bad_characters_summary_and_write_to_file()


def get_breaking_bad_characters_summary_and_write_to_file_wrapper(
    characters: List
) -> None:
    asyncio.run(get_breaking_bad_characters_summary_and_write_to_file(characters))


@duration
def get_breaking_bad_characters_summary_and_write_to_file_multiprocessing() -> None:
    # Takes ~8 seconds
    characters = asyncio.run(get_breaking_bad_characters())

    TOTAL_CHARACTERS = len(characters)
    NUM_CORES = cpu_count()

    CHARS_PER_CORE = floor(TOTAL_CHARACTERS / NUM_CORES)

    futures = []

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for i in range(NUM_CORES - 1):
            futures.append(
                executor.submit(
                    get_breaking_bad_characters_summary_and_write_to_file_wrapper,
                    characters=characters[
                        i * CHARS_PER_CORE : (i + 1) * CHARS_PER_CORE
                    ],
                )
            )

        futures.append(
            executor.submit(
                get_breaking_bad_characters_summary_and_write_to_file_wrapper,
                characters[(i + 1) * CHARS_PER_CORE :],
            )
        )

    concurrent.futures.wait(futures)


async def get_breaking_bad_random_characters_N_times(n: int) -> None:
    # Takes ~67 seconds
    async with AsyncClient() as client:
        for i in range(n):
            await get_breaking_bad_random_character(client)


def get_breaking_bad_random_characters_N_times_wrapper(n: int) -> None:
    asyncio.run(get_breaking_bad_random_characters_N_times(n))


@duration
def get_breaking_bad_random_characters_N_times_multiprocessing(n: int) -> None:
    # Takes ~18 seconds
    NUM_CORES = cpu_count()

    CALLS_PER_CORE = floor(n / NUM_CORES)
    CALLS_FOR_FINAL_CORE = CALLS_PER_CORE + n % CALLS_PER_CORE

    futures = []

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for i in range(NUM_CORES):
            calls = CALLS_PER_CORE if i < NUM_CORES else CALLS_FOR_FINAL_CORE

            futures.append(
                executor.submit(
                    get_breaking_bad_random_characters_N_times_wrapper, calls
                )
            )

    concurrent.futures.wait(futures)


async def get_breaking_bad_random_characters_N_times_multithread(n: int) -> None:
    # Takes 5 seconds
    async with AsyncClient() as client:
        return await asyncio.gather(
            *[get_breaking_bad_random_character(client) for i in range(n)]
        )


if __name__ == "__main__":
    asyncio.run(get_breaking_bad_characters_summary_and_write_to_file_sequential())
    get_breaking_bad_characters_summary_and_write_to_file_multiprocessing()

    with timer("get_breaking_bad_random_characters_N_times_sequential"):
        asyncio.run(get_breaking_bad_random_characters_N_times(250))

    get_breaking_bad_random_characters_N_times_multiprocessing(250)

    with timer("get_breaking_bad_random_characters_N_times_multithread"):
        asyncio.run(get_breaking_bad_random_characters_N_times_multithread(250))

