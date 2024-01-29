from sparkconfgenerator import SparkConfGenerator, DeployMode
from sparkconfgenerator.models import VirtualMachine, Unit


class TestSparkConfGenerator:
    def test_generator_properties(self) -> None:
        properties = SparkConfGenerator(
            driver_instance=VirtualMachine(cores=54, memory=86, unit=Unit.GB),
            worker_instance=VirtualMachine(cores=26, memory=43, unit=Unit.GB),
            dynamic_allocation=True,
            num_worker_instances=6,
            deploy_mode=DeployMode.CLIENT,
        )
        properties = properties.properties

        assert properties.driver_memory == 59002
        assert properties.driver_memory_overhead == 8807
        assert properties.driver_cores == 5
        assert properties.executor_memory == 6451
        assert properties.executor_memory_overhead == 717
        assert properties.executor_cores == 4
        assert properties.executor_instances == 36
        assert properties.default_parallelism == 288
        assert properties.yarn_am_memory == 2048
        assert properties.yarn_am_cores == 1
        assert properties.dynamic_allocation is True
        assert properties.shuffle_service_enabled is True
        assert properties.min_executors == 1
        assert properties.max_executors == 36
        assert properties.initial_executors == 18
        assert properties.cached_executor_idle_timeout == 60
        assert properties.executor_idle_timeout == 60
        assert properties.sql_execution_arrow_pyspark_enabled is True

    def test_generator_properties_as_string(self) -> None:
        properties = SparkConfGenerator(
            driver_instance=VirtualMachine(cores=54, memory=86, unit=Unit.GB),
            worker_instance=VirtualMachine(cores=26, memory=43, unit=Unit.GB),
            dynamic_allocation=True,
            num_worker_instances=6,
            deploy_mode=DeployMode.CLIENT,
        )

        expected = """
        spark.master    yarn
        spark.driver.memory    59002m
        spark.driver.memoryOverhead    8807m
        spark.driver.cores    5
        spark.driver.maxResultSize    40960m
        spark.executor.memory    6451m
        spark.executor.memoryOverhead    717m
        spark.executor.cores    4
        spark.default.parallelism    288
        spark.yarn.am.memory    2048m
        spark.yarn.am.cores    1
        spark.dynamicAllocation.enabled    true
        spark.shuffle.service.enabled    true
        spark.dynamicAllocation.minExecutors    1
        spark.dynamicAllocation.maxExecutors    36
        spark.dynamicAllocation.initialExecutors    18
        spark.dynamicAllocation.cachedExecutorIdleTimeout    60s
        spark.dynamicAllocation.executorIdleTimeout    60s
        spark.sql.execution.arrow.pyspark.enabled    true
        """

        assert properties.get_as_spark_props() == expected
