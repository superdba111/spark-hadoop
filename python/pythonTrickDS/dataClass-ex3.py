# using python 3.10+
from dataclasses import dataclass, field
from math import pi

@dataclass
class Circle:
    x: float = 0
    y: float = 0
    radius: float = 1

    @property
    def circumference(self) -> float:
        return 2 * self.radius * pi

def main() -> None:
    circle = Circle(radius=2)
    print(f"Circumference: {circle.circumference}") #using property removing ()after circumference

if __name__ == '__main__':
    main()
