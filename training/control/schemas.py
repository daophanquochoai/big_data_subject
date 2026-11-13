from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

Device = Literal['cpu', 'cuda']
Status = Literal['queued', 'starting', 'running', 'succeeded', 'failed', 'cancelled']
Schedule = Literal['*/5 * * * *', '0 * * * *', '0 2 * * *']


class Strict(BaseModel):
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False)


class ConfigIn(Strict):
    enabled: bool = False
    schedule: Schedule = '0 2 * * *'
    timezone: Literal['Asia/Ho_Chi_Minh'] = 'Asia/Ho_Chi_Minh'
    device: Device = 'cpu'
    world_size: int = Field(default=2, ge=1, le=32)
    epochs: int = Field(default=12, ge=1, le=100)
    batch_size: int = Field(default=64, ge=8, le=4096)
    learning_rate: float = Field(default=0.003, gt=0, le=1)
    fraction: float = Field(default=0.2, ge=0.01, le=1)


class ConfigOut(ConfigIn):
    next_run_at: datetime | None


class RunIn(Strict):
    request_id: UUID
    device: Device | None = None
    world_size: int | None = Field(default=None, ge=1, le=32)
    epochs: int | None = Field(default=None, ge=1, le=100)
    batch_size: int | None = Field(default=None, ge=8, le=4096)
    learning_rate: float | None = Field(default=None, gt=0, le=1)
    fraction: float | None = Field(default=None, ge=0.01, le=1)


class Metric(Strict):
    epoch: int = Field(ge=1, le=100)
    loss: float = Field(ge=0)
    accuracy: float = Field(ge=0, le=1)
    elapsed_seconds: float = Field(ge=0)
    rul_rmse: float | None = Field(default=None, ge=0)
    failing_f1: float | None = Field(default=None, ge=0, le=1)


class Rank(Strict):
    rank: int = Field(ge=0, le=31)
    device: str = Field(min_length=1, max_length=100)
    status: str = Field(min_length=1, max_length=32)
    epoch: int | None = Field(default=None, ge=0, le=100)
    loss: float | None = Field(default=None, ge=0)


class Log(Strict):
    at: datetime
    level: str = Field(default='info', min_length=1, max_length=16)
    message: str = Field(min_length=1, max_length=4000)


class RunOut(Strict):
    id: int
    run_key: str
    name: str
    status: Status
    source: Literal['manual', 'airflow']
    device: Device
    world_size: int
    epochs: int
    batch_size: int
    learning_rate: float
    fraction: float
    training_image: str
    dataset: str
    completed_epochs: int
    progress: float
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    airflow_run_id: str | None
    container_id: str | None
    error: str | None
    cancel_requested: bool
    artifact_path: str | None
    metrics: list[Metric]
    ranks: list[Rank]
    logs: list[Log]


class DispatchIn(Strict):
    dispatch_key: str = Field(min_length=1, max_length=200)
    run_id: int | None = Field(default=None, ge=1)
    airflow_run_id: str = Field(min_length=1, max_length=250)


class DispatchOut(Strict):
    run: RunOut | None


class EventsIn(Strict):
    status: Status | None = None
    container_id: str | None = Field(default=None, max_length=128)
    completed_epochs: int | None = Field(default=None, ge=0, le=100)
    metrics: list[Metric] = Field(default_factory=list, max_length=100)
    ranks: list[Rank] = Field(default_factory=list, max_length=32)
    logs: list[Log] = Field(default_factory=list, max_length=100)
    error: str | None = Field(default=None, max_length=4000)
    artifact_path: str | None = Field(default=None, max_length=1000)


class Gpu(Strict):
    index: int
    name: str
    memory_total_mb: int
    memory_used_mb: int | None
    utilization_percent: int | None


class AirflowStatus(Strict):
    status: Literal['healthy', 'unavailable']
    url: str
    dag_id: str


class SystemOut(Strict):
    host: str
    cpu_count: int
    gpu_count: int
    gpus: list[Gpu]
    docker_available: bool
    image_available: bool
    training_image_id: str | None = None
    cuda_available: bool
    multi_gpu_available: bool
    airflow: AirflowStatus
    message: str
