from pathlib import Path
from typing import Any

from PIL import Image
import torchvision.transforms.functional as TF

from ..base import InferenceModel
from ..transforms import decode_pil, resize_pil, to_numpy
from ...schemas import ModelSession, ModelTask, ModelType, ModelFormat
from ...config import settings


class Scorer(InferenceModel):
    depends = []
    identity = (ModelType.SCORE, ModelTask.IQA)

    def __init__(self, model_name: str, **model_kwargs: Any) -> None:
        super().__init__(model_name,
                         model_format=ModelFormat.ONNX,
                         **model_kwargs)
        self.size = 384

    @property
    def model_dir(self) -> Path:
        return settings.models_path / self.model_name

    def clear_cache(self) -> None:
        pass

    def download(self) -> None:
        pass

    def _load(self) -> ModelSession:
        self.session = session = self._make_session(self.model_path)

        return session

    def _predict(self, inputs: Image.Image | bytes, **kwargs: Any) -> dict[str, float]:
        image = decode_pil(inputs)
        image = resize_pil(image, self.size)
        img_tensor = TF.to_tensor(image.convert('RGB')).unsqueeze(0)

        # Get ONNX prediction
        [model_input] = self.session.get_inputs()
        ort_inputs = {model_input.name: img_tensor.numpy()}
        score = float(self.session.run(None, ort_inputs)[0][0][0])
        return {"score": score}
