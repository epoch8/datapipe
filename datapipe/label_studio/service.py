from multiprocessing import Process

from datapipe.label_studio.run_server import (
    LabelStudioConfig, start_label_studio_app
)


class LabelStudioService:
    def __init__(
        self,
        label_studio_config: LabelStudioConfig,
    ):
        self.process = None
        self.label_studio_config = label_studio_config

    def run_services(self):
        if self.process is None:
            self.process = Process(
                target=start_label_studio_app,
                kwargs={
                    'label_studio_config': self.label_studio_config
                }
            )
            self.process.start()
        else:
            raise RuntimeError("Process is already started.")

    def terminate_services(self):
        self.process.terminate()
        self.process.join()

    def __del__(self):
        if self.process is not None:
            self.terminate_services()
