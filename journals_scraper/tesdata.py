import json
from dataclasses import dataclass, asdict


class AuthorJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Author):
            return asdict(o)
        return super().default(o)


@dataclass
class Author:
    name: str
    email: str
    orcid: str


def somefunc():
    a = Author(name="Homer", email="Homer@homer", orcid="0000-0000-0000-0000")
    print(dir(somefunc))
    print(locals())
    return a


def main():
    a = somefunc()
    print(json.dumps(a, cls=AuthorJSONEncoder))


if __name__ == "__main__":
    main()
