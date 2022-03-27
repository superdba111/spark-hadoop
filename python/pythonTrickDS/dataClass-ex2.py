# using python 3.10+
import random
import string
from dataclasses import dataclass, field
import timeit
from functools import partial

@dataclass(slots=False)
class Person:
    name: str
    address: str
    email: str

@dataclass(slots=True)
class PersonSlots:
    name: str
    address: str
    email: str

# class PersonEmployee(PersonSlots, EmployeeSlots):
#   pass

def get_set_delete(person: Person | PersonSlots):
    person.address = "123 Main St"
    person.address
    del person.address


def main() -> None:
    person = Person("John", "123 Main St", "john@doe.com")
    person_slots = PersonSlots("John", "123 Main St", "john@doe.com")
    no_slots = min(timeit.repeat(partial(get_set_delete, person), number=1000000))
    slots = min(timeit.repeat(partial(get_set_delete, person), number=1000000))
    print(f"No slots: {no_slots}")
    print(f"Slots: {slots}")
    print(f"% performance improvement: {(no_slots - slots) / no_slots:.2%}")

if __name__ == '__main__':
    main()
