from __future__ import annotations

from loom.core.model import BaseModel, LoomStruct
from loom.core.response import Response


class TestBaseModelStruct:
    def test_base_model_is_a_loom_struct(self) -> None:
        assert issubclass(BaseModel, LoomStruct)

    def test_response_is_a_loom_struct(self) -> None:
        assert issubclass(Response, LoomStruct)
