from typing import Any

from sparkconfgenerator.models import (
    VirtualMachine,
    SparkProperties,
    SparkPropertiesClient,
    DeployMode,
)


class SparkConfGenerator:
    """Generate the optimal Spark Properties for a given setup

    Args:
        driver_instance (VirtualMachine): The driver instance specs
        worker_instance (VirtualMachine): The worker instance specs
        deploy_mode (DeployMode): The deployment mode (client or cluster)
        num_worker_instances (int): The number of worker instances
        dynamic_allocation (bool): Whether dynamic allocation is enabled

    """

    DEPLOY_MODE_MAPPING = {DeployMode.CLIENT: SparkPropertiesClient}

    def __init__(
        self,
        driver_instance: VirtualMachine,
        worker_instance: VirtualMachine,
        deploy_mode: DeployMode,
        num_worker_instances: int,
        dynamic_allocation: bool = True,
    ):
        self.dynamic_allocation = dynamic_allocation
        self.deploy_mode = deploy_mode
        self.worker_instance = worker_instance
        self.driver_instance = driver_instance
        self.num_worker_instances = num_worker_instances

    @property
    def properties(self) -> SparkProperties:
        return self.DEPLOY_MODE_MAPPING[self.deploy_mode](
            worker_instance=self.worker_instance,
            driver_instance=self.driver_instance,
            num_worker_instances=self.num_worker_instances,
        )

    @staticmethod
    def get_enum_val(value: str, enum_obj) -> Any:
        try:
            enum_entry = next(
                mode for mode in enum_obj if mode.value.upper() == value.upper()
            )
        except StopIteration:
            raise ValueError(f"No matching enum_obj for {value}")
        return enum_entry

    def get_as_spark_props(self) -> str:
        return f"""
        spark.master    yarn
        spark.driver.memory    {self.properties.driver_memory}m
        spark.driver.memoryOverhead    {self.properties.driver_memory_overhead}m
        spark.driver.cores    {self.properties.driver_cores}
        spark.driver.maxResultSize    {self.properties.driver_max_result_size}m
        spark.executor.memory    {self.properties.executor_memory}m
        spark.executor.memoryOverhead    {self.properties.executor_memory_overhead}m
        spark.executor.cores    {self.properties.executor_cores}
        spark.default.parallelism    {self.properties.default_parallelism}
        spark.yarn.am.memory    {self.properties.yarn_am_memory}m
        spark.yarn.am.cores    {self.properties.yarn_am_cores}
        spark.dynamicAllocation.enabled    {
        str(self.properties.dynamic_allocation).lower()
        }
        spark.shuffle.service.enabled    {
        str(self.properties.shuffle_service_enabled).lower()
        }
        spark.dynamicAllocation.minExecutors    {self.properties.min_executors}
        spark.dynamicAllocation.maxExecutors    {self.properties.max_executors}
        spark.dynamicAllocation.initialExecutors    {self.properties.initial_executors}
        spark.dynamicAllocation.cachedExecutorIdleTimeout    {
        self.properties.cached_executor_idle_timeout
        }s
        spark.dynamicAllocation.executorIdleTimeout    {self.properties.executor_idle_timeout}s
        spark.sql.execution.arrow.pyspark.enabled    {
        str(self.properties.sql_execution_arrow_pyspark_enabled).lower()
        }
        """
