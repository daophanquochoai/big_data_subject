"""Cấu hình của bộ giả lập. Bất biến: đổi cấu hình là dựng đối tượng mới rồi thay nguyên."""
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from engine.data import SUBSETS

Method = Literal["gauss", "warp", "blend", "parametric"]
Subset = Literal["FD001", "FD002", "FD003", "FD004"]
Probability = Annotated[float, Field(ge=0, le=1)]

AVAILABLE_METHODS = ("gauss", "warp", "blend", "parametric")


class Faults(BaseModel):
    """Xác suất trên từng Bản tin; mỗi Bản tin bốc đúng một kết cục nên tổng không quá 1."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    bad_value: Probability = 0.0
    drop: Probability = 0.0
    late: Probability = 0.0
    duplicate: Probability = 0.0

    @model_validator(mode="after")
    def _total(self):
        if self.bad_value + self.drop + self.late + self.duplicate > 1:
            raise ValueError("tổng xác suất Lỗi chèn phải ≤ 1")
        return self


class Config(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    machines: int = Field(500, ge=1, le=10_000)
    speed: float = Field(1.0, ge=0.1, le=10)  # Chu kỳ mỗi giây của mỗi Máy ảo
    noise: float = Field(0.1, ge=0, le=1)     # độ lệch nhiễu, tính theo σ của cảm biến trong Điều kiện vận hành
    subsets: tuple[Subset, ...] = Field(tuple(SUBSETS), min_length=1)
    method_weights: dict[Method, Annotated[float, Field(ge=0)]] = Field(
        default_factory=lambda: dict.fromkeys(AVAILABLE_METHODS, 1.0))
    warp_range: tuple[Annotated[float, Field(ge=0.3, le=3)], Annotated[float, Field(ge=0.3, le=3)]] = (0.7, 1.5)
    faults: Faults = Faults()
    late_max_s: float = Field(5, ge=1, le=60)
    seed: int = Field(42, ge=0)

    @model_validator(mode="after")
    def _check(self):
        if len(set(self.subsets)) != len(self.subsets):
            raise ValueError("tập con bị lặp")
        if sum(self.method_weights.values()) <= 0:
            raise ValueError("tổng tỷ lệ trộn phương pháp phải > 0")
        if self.warp_range[0] > self.warp_range[1]:
            raise ValueError("khoảng co giãn: min phải ≤ max")
        return self
