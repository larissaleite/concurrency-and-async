import requests
from collections import defaultdict
from bs4 import BeautifulSoup
from utils import duration, write_to_file

QUOTES_API_URL = "https://breaking-bad-quotes.herokuapp.com/v1/quotes"

WIKIPEDIA_PREFIX_URL = "https://en.wikipedia.org/wiki"


def get_breaking_bad_quotes_per_character(num_quotes: int) -> defaultdict:
    quotes = defaultdict(list)
    response = requests.get(f"{QUOTES_API_URL}/{num_quotes}")
    for quote in response.json():
        quotes[quote["author"]].append(quote["quote"])
    return quotes


@duration
def get_wikipedia_character_info(character: str) -> str:
    character = character.replace(" ", "_")
    url = f"{WIKIPEDIA_PREFIX_URL}/{character}_(Breaking_Bad)"

    response = requests.get(url)

    soup = BeautifulSoup(response.text, features="html.parser")

    paragraphs = soup.select("p")
    return "\n".join([paragraph.text for paragraph in paragraphs[:5]])


if __name__ == "__main__":
    quotes_per_character = get_breaking_bad_quotes_per_character(15)

    for character, quotes in quotes_per_character.items():
        wikipedia_info = get_wikipedia_character_info(character)

        quotes_as_txt = "\n".join(quotes)
        file_content = (
            f"{character}\nSummary:\n{wikipedia_info}\nQuotes:\n{quotes_as_txt}"
        )

        write_to_file(file_name=character, file_content=file_content)
