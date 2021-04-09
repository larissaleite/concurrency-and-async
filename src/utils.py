import functools
from pathlib import Path
from time import time
import aiofiles


def duration(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        end = time()
        print(f"Total duration of {func.__name__}: {end - start} seconds.")
        return result

    return wrapper


def duration_async(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start = time()
        result = await func(*args, **kwargs)
        end = time()
        print(f"Total duration of {func.__name__}: {end - start} seconds.")
        return result

    return wrapper


def write_to_file(file_name: str, file_content: str) -> None:
    file_path = Path(__file__).parent / "files" / f"{file_name}.txt"
    with open(file_path, "w+", encoding="utf-8") as f:
        f.write(file_content)


async def write_to_file_async(file_name: str, file_content: str) -> None:
    file_path = Path(__file__).parent / "files" / f"{file_name}.txt"
    async with aiofiles.open(file_path, "w+", encoding="utf-8") as f:
        await f.write(file_content)
