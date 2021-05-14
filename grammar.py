from pyparsing import Literal, Word, ZeroOrMore, Forward, nums, oneOf, Group, alphas, alphanums, Optional, OneOrMore, \
    Group, Combine, CaselessKeyword, printables


class grammar:

    def __init__(self):
        wordChars = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!#$%&\'()*+,-./:;<=>?@[\]^_`{|}~'
        self.Naming = Word(alphas + "_", alphanums + "_")
        self.Separator = Literal(",").suppress()
        self.Sugar = Literal("-:").suppress()
        self.FNumber = Combine(Word("+-" + nums, nums)
                               + Optional("." + Optional(Word(nums)))
                               + Optional(CaselessKeyword("E") + Word("+-" + nums, nums)))
        self.QuotedString = Word(wordChars + " _", wordChars + " _")

    def Body(self):
        return Group(self.Table()) + ZeroOrMore(Group(self.Separator + self.Table()))

    def Table(self):
        return self.Naming \
               + Literal("(").suppress() \
               + Group(self.TableColumn() + ZeroOrMore(self.Separator + self.TableColumn())) \
               + Literal(")").suppress()

    def TableColumn(self):
        return Group(self.Naming + Literal(":") + self.Condition()) \
               | self.Naming

    def Condition(self):
        return (Combine(Literal('"') + self.QuotedString + Literal('"'))) \
               | self.Naming \
               | self.FNumber

    def Header(self):
        return OneOrMore(Group(self.Sugar + self.Event()))

    def Event(self):
        return self.Naming \
               + Literal("(").suppress() \
               + Group(self.EventColumn() + ZeroOrMore(self.Separator + self.EventColumn())) \
               + Literal(")").suppress()

    def EventColumn(self):
        return Group(self.Naming + Literal(":") + self.Condition()) \
               | Group(self.NestedTable()) \
               | self.Naming

    def NestedTable(self):
        return self.Naming + "<" + self.Naming + ZeroOrMore(self.Separator + self.Naming) + ">(" \
               + self.NestedTableColumn() + ZeroOrMore(self.Separator + self.NestedTableColumn()) \
               + Literal(")")

    def NestedTableColumn(self):
        return Group(self.Naming + Literal(":") + self.Naming) \
               | self.Naming

    def Syntax(self):
        mapping_rules = self.Naming + Literal("=").suppress() + Group(self.Body()) + Group(self.Header())
        return mapping_rules
