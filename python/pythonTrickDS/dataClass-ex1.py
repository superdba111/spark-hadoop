import random
import string
from dataclasses import dataclass, field

def generated_id() -> str:
    return "".join(random.choices(string.ascii_uppercase, k=12))

# @dataclass(frozen=False, KW_ONLY=False)
@dataclass(frozen=False)
# @dataclass(match_args=False)
class Person:
    # def __init__(self, name: str, address: str):
    #     self.name = name
    #     self.address = address

    # def __str__(self) -> str:
    #     return f"{self.name}, {self.address}"
    name: str
    address: str
    active: bool = True
    email_addresses: list = field(default_factory=list)
    id: str = field(init=False, default_factory=generated_id)
    _search_string: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._search_string = f"{self.name} {self.address}"

def main() -> None:
    person = Person(name="John", address="123 Main St", active=False)
    print(person.__dict__["name"])
    person.name = "Josh"
    print(person)

if __name__ == '__main__':
    main()
    person1 = Person("John", "123 Main St")
    print(person1)