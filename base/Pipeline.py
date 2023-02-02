from typing import List

from base.Step import Step


class Pipeline:
    def __init__(self, name: str = "", pipeline: List[Step] = None) -> None:
        if pipeline is None:
            pipeline = []
        self._pipeline = pipeline
        self._name = name

    def add_step(self, step: Step) -> None:
        if self._find_step(step):
            print(f'Step {step.id} already exists in {self.name} pipeline')
        else:
            self._pipeline.append(step)

    def remove_step(self, step: Step) -> None:
        if not self._find_step(step):
            print(f'Step {step.id} not found in {self.name} pipeline')
        else:
            self._pipeline.remove(step)

    def do_pipeline(self, path: str = None) -> None:
        print(f'Execution of {self.name} pipeline')
        for step in self._pipeline:
            if step.enable:
                print(f'==STARTING "{step.id}" STEP==')
                step.execute(path)
                print(f'==END "{step.id}" STEP==')

    def _find_step(self, step: Step) -> Step:
        for s in self._pipeline:
            if s.id == step.id:
                return step
        return None

    @property
    def name(self) -> str:
        return "no_name" if self._name == "" else self._name
