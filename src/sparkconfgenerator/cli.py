import click

from sparkconfgenerator import DeployMode
from sparkconfgenerator.models import (
    VirtualMachine,
    Unit,
)
from sparkconfgenerator.generator import SparkConfGenerator


@click.command()
@click.option(
    "--worker-instances", type=int, required=True, help="Number of worker instances"
)
@click.option(
    "--driver-memory", type=int, required=True, help="Driver memory in megabytes"
)
@click.option("--driver-cores", type=int, required=True, help="Number of driver cores")
@click.option(
    "--worker-memory", type=int, required=True, help="Worker memory in megabytes"
)
@click.option("--worker-cores", type=int, required=True, help="Number of worker cores")
@click.option(
    "--unit", type=str, default="mb", help="Memory unit of measurement " "(gb, mb..)"
)
@click.option(
    "--deploy-mode",
    type=str,
    default="CLIENT",
    required=True,
    help="Deploy mode (CLIENT or CLUSTER)",
)
def generate_spark_config(
    worker_instances: int,
    driver_memory: int,
    driver_cores: int,
    worker_memory: int,
    worker_cores: int,
    unit: str,
    deploy_mode: str,
):
    deploy_mode = SparkConfGenerator.get_enum_val(deploy_mode, DeployMode)
    unit = SparkConfGenerator.get_enum_val(unit, Unit)
    worker_vm = VirtualMachine(
        cores=worker_cores, memory=worker_memory, unit=unit  # type: ignore
    )
    driver_vm = VirtualMachine(
        cores=driver_cores, memory=driver_memory, unit=unit  # type: ignore
    )

    spark_conf_generator = SparkConfGenerator(
        driver_instance=driver_vm,
        worker_instance=worker_vm,
        dynamic_allocation=True,
        num_worker_instances=worker_instances,
        deploy_mode=deploy_mode,  # type: ignore
    )
    click.echo(spark_conf_generator.get_as_spark_props())


if __name__ == "__main__":
    generate_spark_config()
