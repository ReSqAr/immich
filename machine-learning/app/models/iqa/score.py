from typing import Any

import torchvision.transforms.functional as TF
from PIL import Image

from ..base import InferenceModel
from ..transforms import decode_pil, resize_pil
from ...schemas import ModelSession, ModelTask, ModelType, ModelFormat

SIZE = 384


class Scorer(InferenceModel):
    depends = []
    identity = (ModelType.SCORE, ModelTask.IQA)

    def __init__(self, model_name: str, **model_kwargs: Any) -> None:
        super().__init__(model_name,
                         model_format=ModelFormat.ONNX,
                         **model_kwargs)
        self.size = SIZE
        self.project_name = "yasinzaehringer" # TODO: remove this once the weights have been uploaded to immich

    def _load(self) -> ModelSession:
        self.session = session = self._make_session(self.model_path)

        return session

    def _predict(self, inputs: Image.Image | bytes, **kwargs: Any) -> dict[str, float]:
        image = decode_pil(inputs)
        image = resize_pil(image, self.size)
        img_tensor = TF.to_tensor(image.convert('RGB')).unsqueeze(0)

        [model_input] = self.session.get_inputs()
        ort_inputs = {model_input.name: img_tensor.numpy()}
        score = float(self.session.run(None, ort_inputs)[0][0][0])
        return {"score": score}
