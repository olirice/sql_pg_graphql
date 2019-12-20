import typing
from enum import Enum, auto


class TokenKind(Enum):
    BANG = auto()
    DOLLAR = auto()
    AMP = auto()
    PAREN_L = auto()
    PAREN_R = auto()
    COLON = auto()
    EQUALS = auto()
    AT = auto()
    BRACKET_L = auto()
    BRACKET_R = auto()
    COMMA = auto()
    BRACE_L = auto()
    BRACE_R = auto()
    PIPE = auto()
    SPREAD = auto()
    NAME = auto()
    INT = auto()
    FLOAT = auto()
    STRING = auto()
    BLOCK_STRING = auto()
    COMMENT = auto()
    WHITESPACE = auto()
    ERROR = auto()


class Token:
    """Equivalent to gql.token"""

    def __init__(self, string):
        string = string[1:-1]
        kind_str, self.contents = string.split(",", 1)
        self.token_kind = TokenKind[kind_str]

    def __repr__(self):
        return f'Token({self.token_kind}, "{self.contents}")'

    @classmethod
    def from_text(cls, text: str) -> typing.List["Token"]:
        """Converts an array of tokens represented as text to a List[Token]
        '{"(WHITESPACE,\"  \")",...}' -> [Token('WHITESPACE', ' '),...]
        """
        text = text[2:-2]
        tokens = []
        for tok_str in text.split('","'):
            tokens.append(Token(tok_str))
        return tokens
