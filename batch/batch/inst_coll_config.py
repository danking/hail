from typing import Union, Tuple, List, Dict, Optional, Any
import re
import logging

from gear import Database

from hailtop.batch_client.parse import worker_type_to_memory_ratio
from hailtop.utils.sorting import none_last

from .globals import MAX_PERSISTENT_SSD_SIZE_GIB
from .utils import (
    adjust_cores_for_memory_request,
    adjust_cores_for_packability,
    round_storage_bytes_to_gib,
    cores_mcpu_to_memory_bytes,
)
from .worker_config import WorkerConfig


log = logging.getLogger('inst_coll_config')


MACHINE_TYPE_REGEX = re.compile('(?P<machine_family>[^-]+)-(?P<machine_type>[^-]+)-(?P<cores>\\d+)')


def machine_type_to_dict(machine_type: str) -> Optional[Dict[str, Any]]:
    match = MACHINE_TYPE_REGEX.search(machine_type)
    if match is None:
        return None
    return match.groupdict()


def requested_storage_bytes_to_actual_storage_gib(storage_bytes):
    if storage_bytes > MAX_PERSISTENT_SSD_SIZE_GIB * 1024 ** 3:
        return None
    if storage_bytes == 0:
        return storage_bytes
    return max(10, round_storage_bytes_to_gib(storage_bytes))


class InstanceCollectionConfig:
    pass


class PoolConfig(InstanceCollectionConfig):
    @staticmethod
    def from_record(record: dict, resource_rates: Dict[str, float]):
        return PoolConfig(
            name=record['name'],
            worker_type=record['worker_type'],
            worker_cores=record['worker_cores'],
            worker_local_ssd_data_disk=record['worker_local_ssd_data_disk'],
            worker_pd_ssd_data_disk_size_gb=record['worker_pd_ssd_data_disk_size_gb'],
            enable_standing_worker=record['enable_standing_worker'],
            standing_worker_cores=record['standing_worker_cores'],
            boot_disk_size_gb=record['boot_disk_size_gb'],
            max_instances=record['max_instances'],
            max_live_instances=record['max_live_instances'],
            resource_rates=resource_rates
        )

    def __init__(
        self,
        name,
        worker_type,
        worker_cores,
        worker_local_ssd_data_disk,
        worker_pd_ssd_data_disk_size_gb,
        enable_standing_worker,
        standing_worker_cores,
        boot_disk_size_gb,
        max_instances,
        max_live_instances,
        resource_rates
    ):
        self.name = name
        self.worker_type = worker_type
        self.worker_cores = worker_cores
        self.worker_local_ssd_data_disk = worker_local_ssd_data_disk
        self.worker_pd_ssd_data_disk_size_gb = worker_pd_ssd_data_disk_size_gb
        self.enable_standing_worker = enable_standing_worker
        self.standing_worker_cores = standing_worker_cores
        self.boot_disk_size_gb = boot_disk_size_gb
        self.max_instances = max_instances
        self.max_live_instances = max_live_instances

        self.worker_config = WorkerConfig.from_pool_config(self, resource_rates)
        self.memory_ratio = worker_type_to_memory_ratio[self.worker_type]

    def memory_bytes_per_cores_mcpu(self, cores_mcpu: int) -> int:
        return cores_mcpu_to_memory_bytes(cores_mcpu, self.worker_type)

    def convert_requests_to_resources(self,
                                      cores_mcpu: int,
                                      memory_bytes: Union[int, str],
                                      storage_bytes: int
                                      ) -> Optional[Tuple[int, int, int]]:
        if isinstance(memory_bytes, str):
            if memory_bytes != self.memory_ratio:
                return None
            memory_bytes = self.memory_bytes_per_cores_mcpu(cores_mcpu)
        cores_mcpu = adjust_cores_for_memory_request(cores_mcpu, memory_bytes, self.worker_type)
        cores_mcpu = adjust_cores_for_packability(cores_mcpu)
        if cores_mcpu > self.worker_cores * 1000:
            return None
        storage_gib = requested_storage_bytes_to_actual_storage_gib(storage_bytes)
        memory_bytes = cores_mcpu_to_memory_bytes(cores_mcpu, self.worker_type)
        return (cores_mcpu, memory_bytes, storage_gib)

    def cost_and_resources_from_request(self,
                                        cores_mcpu: int,
                                        memory_bytes: Union[int, str],
                                        storage_bytes: int
                                        ) -> Optional[ResourcesAndCost]:
        fulfilled_resources = self.convert_requests_to_resources(cores_mcpu, memory_bytes, storage_bytes)
        if fulfilled_resources is not None:
            return ResourcesAndCost(fulfilled_resources,
                                    self.worker_config.cost_per_hour(cores_mcpu, memory_bytes, storage_bytes))
        return None


class ResourcesAndCost:
    def __init__(self, resources: Tuple[int, int, int], cost: float):
        self.resources = resources
        self.cost = cost


class JobPrivateInstanceManagerConfig(InstanceCollectionConfig):
    @staticmethod
    def from_record(record):
        return JobPrivateInstanceManagerConfig(
            record['name'], record['boot_disk_size_gb'], record['max_instances'], record['max_live_instances']
        )

    def __init__(self, name, boot_disk_size_gb, max_instances, max_live_instances):
        self.name = name
        self.boot_disk_size_gb = boot_disk_size_gb
        self.max_instances = max_instances
        self.max_live_instances = max_live_instances

    def convert_requests_to_resources(self,
                                      machine_type: str,
                                      storage_bytes: int
                                      ) -> Optional[Tuple[int, int, int, str]]:
        # minimum storage for a GCE instance is 10Gi
        storage_gib = max(10, requested_storage_bytes_to_actual_storage_gib(storage_bytes))

        machine_type_dict = machine_type_to_dict(machine_type)
        if machine_type_dict is None:
            return None
        cores = int(machine_type_dict['cores'])
        cores_mcpu = cores * 1000

        memory_bytes = cores_mcpu_to_memory_bytes(cores_mcpu, machine_type_dict['machine_type'])

        return (cores_mcpu, memory_bytes, storage_gib, machine_type)


class InstanceCollectionConfigs:
    def __init__(self, app):
        self.app = app
        self.db: Database = app['db']
        self.name_config: Dict[str, InstanceCollectionConfig] = {}
        self.name_pool_config: Dict[str, PoolConfig] = {}
        self.jpim_config: Optional[JobPrivateInstanceManagerConfig] = None

    async def async_init(self):
        await self.refresh()

    async def refresh(self):
        log.info('loading inst coll configs and resource rates from db')
        resource_rates: Dict[str, float] = {
            record['resource']: record['rate']
            async for record in self.db.execute_and_fetchall('SELECT * FROM resources;')}
        records = self.db.execute_and_fetchall(
            '''
SELECT inst_colls.*, pools.*
FROM inst_colls
LEFT JOIN pools ON inst_colls.name = pools.name;
'''
        )
        async for record in records:
            is_pool = bool(record['is_pool'])
            if is_pool:
                config = PoolConfig.from_record(record, resource_rates)
                self.name_pool_config[config.name] = config
            else:
                config = JobPrivateInstanceManagerConfig.from_record(record)
                self.jpim_config = config

            self.name_config[config.name] = config

        assert self.jpim_config is not None


    def select_pool(self,
                    cores_mcpu: int,
                    memory_bytes: Union[int, str],
                    storage_bytes: int
                    ) -> Optional[ResourcesAndCost]:
        resources_and_cost: List[Optional[ResourcesAndCost]] = [
            pool.cost_and_resources_from_request(cores_mcpu, memory_bytes, storage_bytes)
            for pool in self.name_pool_config.values()]
        return min(resources_and_cost, key=none_last(lambda x: x.cost))

    def select_job_private(self,
                           machine_type: str,
                           storage_bytes: int
                           ) -> Optional[Tuple[int, int, int, str]]:
        assert self.jpim_config is not None
        return self.jpim_config.convert_requests_to_resources(machine_type, storage_bytes)
