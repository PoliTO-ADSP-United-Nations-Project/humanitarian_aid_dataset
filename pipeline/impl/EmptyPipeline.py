
from base.Pipeline import Pipeline


class EmptyPipeline(Pipeline):

    def __init__(self) -> None:
        super().__init__("Empty_Pipeline", [])

