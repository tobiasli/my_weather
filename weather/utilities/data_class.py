"""Class for representing dictionaries as objects (structs)."""
import typing


class DataClass:
    """
    Class for object (struct) representation of dictionaries.
    """
    def __init__(self, **kwargs) -> None:
        self.dict = kwargs
        self.dir = sorted([key.lower() for key in kwargs])

    def __getattr__(self, item: str) -> str:
        return self.dict[item]

    def __dir__(self) -> typing.List[str]:
        return self.dir + dir(super(type(self))) + ['dir', 'dict']

    def __contains__(self, item: typing.Any):
        return item in self.dict

    def __str__(self) -> str:
        args = [f'{k}={repr(v)}' for k, v in self.dict.items()]
        string = ", ".join(args)
        return f'{type(self).__name__}({string})'

    def __repr__(self) -> str:
        args = [f'{k}={repr(v)}' for k, v in self.dict.items()]
        string = ", ".join(args)
        if len(string) > 200:
            trailing = '... '
        else:
            trailing = ''
        return f'{type(self).__name__}({string[0:200]}{trailing})'

    def __len__(self) -> int:
        return len(self.dict)
