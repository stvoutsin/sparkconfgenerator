from typing import Protocol


class SparkProperties(Protocol):
    """Spark properties protocol, defines the specs for a Spark properties object"""

    @property
    def driver_memory(self) -> int:
        ...

    @property
    def driver_cores(self) -> int:
        ...

    @property
    def driver_memory_overhead(self) -> int:
        ...

    @property
    def driver_max_result_size(self) -> int:
        ...

    @property
    def total_executor_memory(self) -> int:
        ...

    @property
    def executor_memory(self) -> int:
        ...

    @property
    def executor_memory_overhead(self) -> int:
        ...

    @property
    def executor_cores(self) -> int:
        ...

    @property
    def executor_instances(self) -> int:
        ...

    @property
    def default_parallelism(self) -> int:
        ...

    @property
    def yarn_am_memory(self) -> int:
        ...

    @property
    def yarn_am_cores(self) -> int:
        ...

    @property
    def min_executors(self) -> int:
        ...

    @property
    def max_executors(self) -> int:
        ...

    @property
    def initial_executors(self) -> int:
        ...

    @property
    def cached_executor_idle_timeout(self) -> int:
        ...

    @property
    def executor_idle_timeout(self) -> int:
        ...

    @property
    def sql_execution_arrow_pyspark_enabled(self) -> int:
        ...

    @property
    def shuffle_service_enabled(self) -> bool:
        ...

    @property
    def dynamic_allocation(self) -> bool:
        ...
