import random

NAME_CHARS: str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
EXT_CHARS: str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

class Rander:
    @staticmethod
    def random_name() -> str:
        length = random.randint(1, 8)
        return ''.join(random.choices(NAME_CHARS, k=length)).rstrip()

    @staticmethod
    def random_extension() -> str:
        length = random.randint(0, 3)
        return ''.join(random.choices(EXT_CHARS, k=length)).rstrip()

    @staticmethod
    def generate_83_name() -> str:
        name = Rander.random_name()
        ext = Rander.random_extension()
        return f"{name}.{ext}" if ext else name

    @staticmethod
    def set_seed(seed: int = 123456789) -> None:
        random.seed(seed)