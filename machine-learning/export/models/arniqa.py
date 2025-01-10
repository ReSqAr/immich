from pathlib import Path
import torch
import torch.nn.functional as F
from pyiqa.archs.arniqa_arch import ARNIQA, IMAGENET_DEFAULT_MEAN, IMAGENET_DEFAULT_STD

from .util import get_model_path


# https://github.com/chaofengc/IQA-PyTorch/blob/main/pyiqa/archs/arniqa_arch.py
# https://huggingface.co/chaofengc/IQA-PyTorch-Weights/tree/main

class ARNIQAWrapper(torch.nn.Module):
    def __init__(self, model: ARNIQA):
        super().__init__()
        self.model = model

        # Extract regressor params to avoid JIT issues
        state_dict = model.regressor.state_dict()
        self.register_parameter('weights', torch.nn.Parameter(state_dict['weights']))
        self.register_parameter('biases', torch.nn.Parameter(state_dict['biases']))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x, x_ds = self.model._preprocess(x)

        # Use model's encoder
        f = F.normalize(self.model.encoder(x), dim=1)
        f_ds = F.normalize(self.model.encoder(x_ds), dim=1)
        f_combined = torch.hstack((f, f_ds)).view(-1, self.model.feat_dim * 2)

        # Replace JIT regressor sk2torch.linear_model.TorchLinearRegression with manual linear regressor
        score = F.linear(f_combined, self.weights.view(1, -1), self.biases)

        # Use model's score scaling
        return self.model._scale_score(score)


def to_onnx(model_name: str, output_dir: Path| str) -> Path:
    output_dir = Path(output_dir) / "score"
    output_dir.mkdir(exist_ok=True, parents=True)
    output_path = get_model_path(output_dir)

    arniqa, regressor_dataset = model_name.split("-", 1)
    assert arniqa == "arniqa"
    model = ARNIQA(regressor_dataset=regressor_dataset)
    model.eval()

    wrapped_model = ARNIQAWrapper(model)
    dummy_input = torch.randn(1, 3, 224, 224)

    torch.onnx.export(
        wrapped_model,
        dummy_input,
        output_path.as_posix(),
        input_names=["input"],
        output_names=["output"],
        opset_version=17,
        dynamic_axes={
            "input": {0: "batch_size", 2: "height", 3: "width"},
            "output": {0: "batch_size"}
        },
    )

    return output_path
