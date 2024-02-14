class ZoekParameterOTL:
    def __init__(self, size: int = 100, from_cursor: str | None = None, expansion_field_list: list = None,
                 filter_dict: dict = None):
        self.size = size
        self.from_cursor = from_cursor
        self.expansion_field_list = expansion_field_list
        self.filter_dict = filter_dict
        if self.filter_dict is None:
            self.filter_dict = {}

    def to_dict(self):
        return {
            "size": self.size,
            "fromCursor": self.from_cursor,
            "expansions": {
                "fields": self.expansion_field_list
            },
            'filters': self.filter_dict
        }
