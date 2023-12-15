from dataclasses import dataclass
import math
from ..utils import Utils
from enum import Enum, auto


@dataclass
class VirtualMachine:
    """Virtual Machine specs
    Args:
        cores (int, required): Number of cores
        memory (int, required): Memory in GB
    """

    cores: int
    memory: int


class SparkPropertiesClient:
    """Spark properties with client deploy mode

    Args:
        driver_instance (VirtualMachine): The driver instance specs
        worker_instance (VirtualMachine): The worker instance specs
        num_worker_instances (int): The number of worker instances
    """

    DEFAULT_MAX_RESULT_SIZE = 40960
    DYNAMIC_ALLOCATION = True

    def __init__(
        self,
        worker_instance: VirtualMachine,
        driver_instance: VirtualMachine,
        num_worker_instances: int,
    ):
        self.num_instances = num_worker_instances
        self.worker_vm_cores = worker_instance.cores
        self.worker_vm_memory = worker_instance.memory
        self.driver_vm_cores = driver_instance.cores
        self.driver_vm_memory = driver_instance.memory

    @property
    @Utils.gb_to_mb
    def driver_memory(self) -> int:
        return math.floor(self.driver_vm_memory * 0.67)

    @property
    def driver_cores(self) -> int:
        return min(self.driver_vm_cores, 5)

    @property
    @Utils.gb_to_mb
    def driver_memory_overhead(self) -> int:
        return math.ceil(self.driver_vm_memory * 0.1)

    @property
    def driver_max_result_size(self) -> int:
        return self.DEFAULT_MAX_RESULT_SIZE

    @property
    def total_executor_memory(self):
        return math.floor((self.worker_vm_memory - 1) / self.executor_per_node)

    @property
    @Utils.gb_to_mb
    def executor_memory(self) -> int:
        return math.floor(self.total_executor_memory * 0.9)

    @property
    @Utils.gb_to_mb
    def executor_memory_overhead(self) -> int:
        return math.ceil(self.total_executor_memory * 0.1)

    @property
    def executor_cores(self) -> int:
        return 4 if self.worker_vm_cores > 4 else max(self.worker_vm_cores - 1, 1)

    @property
    def executor_per_node(self) -> int:
        return math.floor((self.worker_vm_cores - 1) / self.executor_cores)

    @property
    def executor_instances(self) -> int:
        return self.executor_per_node * self.num_instances

    @property
    def default_parallelism(self) -> int:
        return self.executor_instances * self.executor_cores * 2

    @property
    def yarn_am_memory(self) -> int:
        return 2048

    @property
    def yarn_am_cores(self) -> int:
        return 1

    @property
    def min_executors(self) -> int:
        return 1

    @property
    def max_executors(self) -> int:
        return self.executor_instances

    @property
    def initial_executors(self) -> int:
        return int(self.max_executors / 2)

    @property
    def cached_executor_idle_timeout(self) -> int:
        return 60

    @property
    def executor_idle_timeout(self) -> int:
        return 60

    @property
    def sql_execution_arrow_pyspark_enabled(self) -> int:
        return True

    @property
    def shuffle_service_enabled(self) -> bool:
        return True

    @property
    def dynamic_allocation(self) -> bool:
        return self.DYNAMIC_ALLOCATION


class DeployMode(Enum):
    """Deploy Mode for Spark (Client or Cluster)"""

    CLIENT = auto()
    CLUSTER = auto()
