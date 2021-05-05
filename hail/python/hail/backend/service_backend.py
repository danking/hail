from typing import Optional, BinaryIO
import struct
import os
import uuid
import aiohttp
import json
import warnings
import logging

from hail.context import TemporaryDirectory, tmp_dir
from hail.utils import FatalError
from hail.expr.types import dtype, tvoid
from hail.expr.table_type import ttable
from hail.expr.matrix_type import tmatrix
from hail.expr.blockmatrix_type import tblockmatrix

from hailtop.config import get_deploy_config, get_user_config, DeployConfig
from hailtop.auth import service_auth_headers, get_tokens
from hailtop.utils import async_to_blocking, retry_transient_errors, secret_alnum_string, TransientError
from hail.ir.renderer import CSERenderer

from hailtop.batch_client import client as hb

from .backend import Backend
from ..hail_logging import PythonOnlyLogger
from ..fs.google_fs import GoogleCloudStorageFS


log = logging.getLogger('backend.service_backend')


def write_int(io: BinaryIO, v: int):
    io.write(struct.pack('<i', v))

def write_long(io: BinaryIO, v: int):
    io.write(struct.pack('<q', v))

def write_bytes(io: BinaryIO, b: bytes):
    n = len(b)
    write_int(io, n)
    io.write(b)

def write_str(io: BinaryIO, s: str):
    write_bytes(io, s.encode('utf-8'))

class EndOfStream(TransientError):
    pass

def read(io: BinaryIO, n: int) -> bytes:
    b = bytearray()
    left = n
    while left > 0:
        t = io.read(left)
        if not t:
            log.warning(f'unexpected EOS, Java violated protocol ({b})')
            raise EndOfStream()
        left -= len(t)
        b.extend(t)
    return b

def read_byte(io: BinaryIO) -> int:
    b = read(io, 1)
    return b[0]

def read_bool(io: BinaryIO) -> bool:
    return read_byte(io) != 0

def read_int(io: BinaryIO) -> int:
    b = read(io, 4)
    return struct.unpack('<i', b)[0]

def read_long(io: BinaryIO) -> int:
    b = read(io, 8)
    return struct.unpack('<q', b)[0]

def read_bytes(io: BinaryIO) -> bytes:
    n = read_int(io)
    return read(io, n)

def read_str(io: BinaryIO) -> str:
    b = read_bytes(io)
    return b.decode('utf-8')

class ServiceBackend(Backend):
    LOAD_REFERENCES_FROM_DATASET = 1
    VALUE_TYPE = 2
    TABLE_TYPE = 3
    MATRIX_TABLE_TYPE = 4
    BLOCK_MATRIX_TYPE = 5
    REFERENCE_GENOME = 6
    EXECUTE = 7
    FLAGS = 8
    GET_FLAG = 9
    UNSET_FLAG = 10
    SET_FLAG = 11
    ADD_USER = 12
    GOODBYE = 254

    def __init__(self,
                 billing_project: str = None,
                 bucket: str = None,
                 *,
                 deploy_config=None,
                 skip_logging_configuration=None,
                 disable_progress_bar: bool = True):
        if billing_project is None:
            billing_project = get_user_config().get('batch', 'billing_project', fallback=None)
        if billing_project is None:
            billing_project = os.environ.get('HAIL_BILLING_PROJECT')
        if billing_project is None:
            raise ValueError(
                "No billing project.  Call 'init_service' with the billing "
                "project, set the HAIL_BILLING_PROJECT environment variable, "
                "or run 'hailctl config set batch/billing_project "
                "MY_BILLING_PROJECT'"
            )

        if bucket is None:
            bucket = get_user_config().get('batch', 'bucket', fallback=None)
        if bucket is None:
            bucket = os.environ.get('HAIL_BUCKET')
        if bucket is None:
            raise ValueError(
                'the bucket parameter of ServiceBackend must be set '
                'or run `hailctl config set batch/bucket '
                'MY_BUCKET`'
            )

        self.billing_project = billing_project
        self.bucket = bucket
        self._fs = GoogleCloudStorageFS()
        tokens = get_tokens()
        deploy_config = deploy_config or get_deploy_config()
        self.session_id = tokens.namespace_token_or_error(deploy_config.service_ns('batch'))
        self.bc = hb.BatchClient(self.billing_project)
        self.disable_progress_bar = disable_progress_bar

    @property
    def fs(self) -> GoogleCloudStorageFS:
        return self._fs

    @property
    def logger(self):
        return log

    @property
    def stop(self):
        pass

    def render(self, ir):
        r = CSERenderer()
        assert len(r.jirs) == 0
        return r(ir)

    def execute(self, ir, timed=False):
        result = self._execute(ir)
        if timed:
            return result, dict()
        return result

    def _execute(self, ir):
        token = secret_alnum_string()
        with TemporaryDirectory(ensure_exists=False) as dir:
            with self.fs.open(dir + '/in', 'wb') as infile:
                write_int(infile, ServiceBackend.EXECUTE)
                write_str(infile, tmp_dir())
                write_str(infile, self.session_id)
                write_str(infile, self.billing_project)
                write_str(infile, self.bucket)
                write_str(infile, self.render(ir))
                write_str(infile, token)

            bb = self.bc.create_batch(token=token)
            bb.create_jvm_job([
                'is.hail.backend.service.ServiceBackendSocketAPI2',
                os.environ['HAIL_SHA'],
                os.environ['HAIL_JAR_URL'],
                dir + '/in',
                dir + '/out',
            ], mount_tokens=True)
            b = bb.submit()
            status = b.wait(disable_progress_bar=self.disable_progress_bar)
            if status['n_succeeded'] != 1:
                raise ValueError(f'batch failed {status} {b.log()}')


            with self.fs.open(dir + '/out', 'rb') as outfile:
                success = read_bool(outfile)
                if success:
                    s = read_str(outfile)
                    try:
                        resp = json.loads(s)
                    except json.decoder.JSONDecodeError as err:
                        raise ValueError(f'could not decode {s}') from err
                else:
                    jstacktrace = read_str(outfile)
                    raise ValueError(jstacktrace)

        typ = dtype(resp['type'])
        if typ == tvoid:
            return None
        return typ._convert_from_json_na(resp['value'])

    def value_type(self, ir):
        token = secret_alnum_string()
        with TemporaryDirectory(ensure_exists=False) as dir:
            with self.fs.open(dir + '/in', 'wb') as infile:
                write_int(infile, ServiceBackend.VALUE_TYPE)
                write_str(infile, tmp_dir())
                write_str(infile, self.render(ir))

            bb = self.bc.create_batch(token=token)
            bb.create_jvm_job([
                'is.hail.backend.service.ServiceBackendSocketAPI2',
                os.environ['HAIL_SHA'],
                os.environ['HAIL_JAR_URL'],
                dir + '/in',
                dir + '/out',
            ], mount_tokens=True)
            b = bb.submit()
            status = b.wait(disable_progress_bar=self.disable_progress_bar)
            if status['n_succeeded'] != 1:
                raise ValueError(f'batch failed {status} {b.log()}')


            with self.fs.open(dir + '/out', 'rb') as outfile:
                success = read_bool(outfile)
                if success:
                    s = read_str(outfile)
                    try:
                        return dtype(json.loads(s))
                    except json.decoder.JSONDecodeError as err:
                        raise ValueError(f'could not decode {s}') from err
                else:
                    jstacktrace = read_str(outfile)
                    raise ValueError(jstacktrace)

    def table_type(self, tir):
        token = secret_alnum_string()
        with TemporaryDirectory(ensure_exists=False) as dir:
            with self.fs.open(dir + '/in', 'wb') as infile:
                write_int(infile, ServiceBackend.TABLE_TYPE)
                write_str(infile, tmp_dir())
                write_str(infile, self.render(tir))

            bb = self.bc.create_batch(token=token)
            bb.create_jvm_job([
                'is.hail.backend.service.ServiceBackendSocketAPI2',
                os.environ['HAIL_SHA'],
                os.environ['HAIL_JAR_URL'],
                dir + '/in',
                dir + '/out',
            ], mount_tokens=True)
            b = bb.submit()
            status = b.wait(disable_progress_bar=self.disable_progress_bar)
            if status['n_succeeded'] != 1:
                raise ValueError(f'batch failed {status} {b.log()}')


            with self.fs.open(dir + '/out', 'rb') as outfile:
                success = read_bool(outfile)
                if success:
                    s = read_str(outfile)
                    try:
                        return ttable._from_json(json.loads(s))
                    except json.decoder.JSONDecodeError as err:
                        raise ValueError(f'could not decode {s}') from err
                else:
                    jstacktrace = read_str(outfile)
                    raise ValueError(jstacktrace)

    def matrix_type(self, mir):
        token = secret_alnum_string()
        with TemporaryDirectory(ensure_exists=False) as dir:
            with self.fs.open(dir + '/in', 'wb') as infile:
                write_int(infile, ServiceBackend.MATRIX_TABLE_TYPE)
                write_str(infile, tmp_dir())
                write_str(infile, self.render(mir))

            bb = self.bc.create_batch(token=token)
            bb.create_jvm_job([
                'is.hail.backend.service.ServiceBackendSocketAPI2',
                os.environ['HAIL_SHA'],
                os.environ['HAIL_JAR_URL'],
                dir + '/in',
                dir + '/out',
            ], mount_tokens=True)
            b = bb.submit()
            status = b.wait(disable_progress_bar=self.disable_progress_bar)
            if status['n_succeeded'] != 1:
                raise ValueError(f'batch failed {status} {b.log()}')


            with self.fs.open(dir + '/out', 'rb') as outfile:
                success = read_bool(outfile)
                if success:
                    s = read_str(outfile)
                    try:
                        return tmatrix._from_json(json.loads(s))
                    except json.decoder.JSONDecodeError as err:
                        raise ValueError(f'could not decode {s}') from err
                else:
                    jstacktrace = read_str(outfile)
                    raise ValueError(jstacktrace)

    def blockmatrix_type(self, bmir):
        token = secret_alnum_string()
        with TemporaryDirectory(ensure_exists=False) as dir:
            with self.fs.open(dir + '/in', 'wb') as infile:
                write_int(infile, ServiceBackend.BLOCK_MATRIX_TYPE)
                write_str(infile, tmp_dir())
                write_str(infile, self.render(bmir))

            bb = self.bc.create_batch(token=token)
            bb.create_jvm_job([
                'is.hail.backend.service.ServiceBackendSocketAPI2',
                os.environ['HAIL_SHA'],
                os.environ['HAIL_JAR_URL'],
                dir + '/in',
                dir + '/out',
            ], mount_tokens=True)
            b = bb.submit()
            status = b.wait(disable_progress_bar=self.disable_progress_bar)
            if status['n_succeeded'] != 1:
                raise ValueError(f'batch failed {status} {b.log()}')


            with self.fs.open(dir + '/out', 'rb') as outfile:
                success = read_bool(outfile)
                if success:
                    s = read_str(outfile)
                    try:
                        return tblockmatrix._from_json(json.loads(s))
                    except json.decoder.JSONDecodeError as err:
                        raise ValueError(f'could not decode {s}') from err
                else:
                    jstacktrace = read_str(outfile)
                    raise ValueError(jstacktrace)

    def add_reference(self, config):
        raise NotImplementedError("ServiceBackend does not support 'add_reference'")

    def from_fasta_file(self, name, fasta_file, index_file, x_contigs, y_contigs, mt_contigs, par):
        raise NotImplementedError("ServiceBackend does not support 'from_fasta_file'")

    def remove_reference(self, name):
        raise NotImplementedError("ServiceBackend does not support 'remove_reference'")

    def get_reference(self, name):
        token = secret_alnum_string()
        with TemporaryDirectory(ensure_exists=False) as dir:
            with self.fs.open(dir + '/in', 'wb') as infile:
                write_int(infile, ServiceBackend.REFERENCE_GENOME)
                write_str(infile, tmp_dir())
                write_str(infile, name)

            bb = self.bc.create_batch(token=token)
            bb.create_jvm_job([
                'is.hail.backend.service.ServiceBackendSocketAPI2',
                os.environ['HAIL_SHA'],
                os.environ['HAIL_JAR_URL'],
                dir + '/in',
                dir + '/out',
            ], mount_tokens=True)
            b = bb.submit()
            status = b.wait(disable_progress_bar=self.disable_progress_bar)
            if status['n_succeeded'] != 1:
                raise ValueError(f'batch failed {status} {b.log()}')

            with self.fs.open(dir + '/out', 'rb') as outfile:
                success = read_bool(outfile)
                if success:
                    s = read_str(outfile)
                    try:
                        # FIXME: do we not have to parse the result?
                        return json.loads(s)
                    except json.decoder.JSONDecodeError as err:
                        raise ValueError(f'could not decode {s}') from err
                else:
                    jstacktrace = read_str(outfile)
                    raise ValueError(jstacktrace)

    def load_references_from_dataset(self, path):
        token = secret_alnum_string()
        with TemporaryDirectory(ensure_exists=False) as dir:
            with self.fs.open(dir + '/in', 'wb') as infile:
                write_int(infile, ServiceBackend.LOAD_REFERENCES_FROM_DATASET)
                write_str(infile, tmp_dir())
                write_str(infile, self.billing_project)
                write_str(infile, self.bucket)
                write_str(infile, path)

            bb = self.bc.create_batch(token=token)
            bb.create_jvm_job([
                'is.hail.backend.service.ServiceBackendSocketAPI2',
                os.environ['HAIL_SHA'],
                os.environ['HAIL_JAR_URL'],
                dir + '/in',
                dir + '/out',
            ], mount_tokens=True)
            b = bb.submit()
            status = b.wait(disable_progress_bar=self.disable_progress_bar)
            if status['n_succeeded'] != 1:
                raise ValueError(f'batch failed {status} {b.log()}')


            with self.fs.open(dir + '/out', 'rb') as outfile:
                success = read_bool(outfile)
                if success:
                    s = read_str(outfile)
                    try:
                        # FIXME: do we not have to parse the result?
                        return json.loads(s)
                    except json.decoder.JSONDecodeError as err:
                        raise ValueError(f'could not decode {s}') from err
                else:
                    jstacktrace = read_str(outfile)
                    raise ValueError(jstacktrace)

    def add_sequence(self, name, fasta_file, index_file):
        raise NotImplementedError("ServiceBackend does not support 'add_sequence'")

    def remove_sequence(self, name):
        raise NotImplementedError("ServiceBackend does not support 'remove_sequence'")

    def add_liftover(self, name, chain_file, dest_reference_genome):
        raise NotImplementedError("ServiceBackend does not support 'add_liftover'")

    def remove_liftover(self, name, dest_reference_genome):
        raise NotImplementedError("ServiceBackend does not support 'remove_liftover'")

    def parse_vcf_metadata(self, path):
        raise NotImplementedError("ServiceBackend does not support 'parse_vcf_metadata'")

    def index_bgen(self, files, index_file_map, rg, contig_recoding, skip_invalid_loci):
        raise NotImplementedError("ServiceBackend does not support 'index_bgen'")

    def import_fam(self, path: str, quant_pheno: bool, delimiter: str, missing: str):
        raise NotImplementedError("ServiceBackend does not support 'import_fam'")

    def register_ir_function(self, name, type_parameters, argument_names, argument_types, return_type, body):
        raise NotImplementedError("ServiceBackend does not support 'register_ir_function'")

    def persist_ir(self, ir):
        raise NotImplementedError("ServiceBackend does not support 'persist_ir'")
