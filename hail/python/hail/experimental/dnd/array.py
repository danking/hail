import json
import numpy as np
from typing import Optional, Tuple

from hail.utils.java import Env
from hail.utils import range_table, new_temp_file
from hail import nd
from hail.matrixtable import MatrixTable
from hail.table import Table

import hail as hl


def array(mt: MatrixTable, entry_field: str, *, block_size=None) -> 'DNDArray':
    return DNDArray.from_matrix_table(mt, entry_field, block_size=block_size)


def read(fname: str) -> 'DNDArray':
    # read without good partitioning, just to get the globals
    a = DNDArray(hl.read_table(fname))
    t = hl.read_table(fname, _intervals=[
        hl.Interval(hl.Struct(**{a.x_field: x, a.y_field: y}),
                    hl.Struct(**{a.x_field: x, a.y_field: y + 1}))
        for x in range(a.n_block_rows)
        for y in range(a.n_block_cols)])
    return DNDArray(t)


class DNDArray:
    """An distributed n-dimensional array.

    Notes
    -----

    :class:`.DNDArray` makes extensive use of :meth:`.init`'s ``tmp_dir``
    parameter to write intermediate results. We advise you to regularly clean up
    that directory. If it is a bucket in Google Cloud Storage, you can use a
    retention policy to automatically clean it up
    """

    default_block_size = 4096
    # FIXME: make lz4fast
    fast_codec_spec = json.dumps({
        "name": "BlockingBufferSpec",
        "blockSize": 64 * 1024,
        "child": {
            "name": "LZ4FastBlockBufferSpec",
            "blockSize": 64 * 1024,
            "child": {
                "name": "StreamBlockBufferSpec"}}})

    @staticmethod
    def from_matrix_table(
            mt: MatrixTable,
            entry_field: str,
            *,
            n_partitions: Optional[int] = None,
            block_size: Optional[int] = None
    ) -> 'DNDArray':
        if n_partitions is None:
            n_partitions = mt.n_partitions()
        if block_size is None:
            block_size = DNDArray.default_block_size
        if n_partitions == 0:
            assert mt.count_cols() == 0
            assert mt.count_rows() == 0
            t = range_table(0, 0)
            t = t.annotate(x=0, y=0, block=nd.array([]).reshape((0, 0)))
            t = t.select_globals(
                x_field='x',
                y_field='y',
                n_rows=0,
                n_cols=0,
                n_block_rows=0,
                n_block_cols=0,
                block_size=0)
            return DNDArray(t)

        assert 'y' not in mt.row
        assert 'x' not in mt.row
        assert 'block' not in mt.row

        n_rows, n_cols = mt.count()
        n_block_rows = (n_rows + block_size - 1) // block_size
        n_block_cols = (n_cols + block_size - 1) // block_size
        entries, cols, row_index, col_blocks = (Env.get_uid() for _ in range(4))
        mt = (mt
              .select_globals()
              .select_rows()
              .select_cols()
              .add_row_index(row_index)
              .localize_entries(entries, cols))
        # FIXME: remove when ndarray support structs
        mt = mt.annotate(**{entries: mt[entries][entry_field]})
        mt = mt.annotate(
            **{col_blocks: hl.range(n_block_cols).map(
                lambda y: hl.struct(
                    y=y,
                    entries=mt[entries][(y * block_size):((y + 1) * block_size)]))}
        )
        mt = mt.explode(col_blocks)
        mt = mt.select(row_index, **mt[col_blocks])
        mt = mt.annotate(x=hl.int(mt[row_index] // block_size))
        mt = mt.key_by(mt.x, mt.y)
        mt = mt.group_by(mt.x, mt.y).aggregate(
            entries=hl.sorted(
                hl.agg.collect(hl.struct(row_index=mt[row_index], entries=mt.entries)),
                key=lambda x: x.row_index
            ).map(lambda x: x.entries))
        mt = mt.select(block=hl.nd.array(mt.entries))
        mt = mt.select_globals(
            x_field='x',
            y_field='y',
            n_rows=n_rows,
            n_cols=n_cols,
            n_block_rows=n_block_rows,
            n_block_cols=n_block_cols,
            block_size=block_size)
        fname = new_temp_file()
        mt = mt.key_by(mt.x, mt.y)
        mt.write(fname, _codec_spec=DNDArray.fast_codec_spec)
        t = hl.read_table(fname, _intervals=[
            hl.Interval(hl.Struct(x=x, y=y),
                        hl.Struct(x=x, y=y + 1))
            for x in range(n_block_rows)
            for y in range(n_block_cols)])
        return DNDArray(t)

    def __init__(self, t: Table) -> 'DNDArray':
        assert 'x' in t.row
        assert 'y' in t.row
        assert 'block' in t.row

        self.m: Table = t

        dimensions = t.globals.collect()[0]
        self.x_field: str = dimensions.x_field
        self.y_field: str = dimensions.y_field
        self.n_rows: int = dimensions.n_rows
        self.n_cols: int = dimensions.n_cols
        self.n_block_rows: int = dimensions.n_block_rows
        self.n_block_cols: int = dimensions.n_block_cols
        self.block_size: int = dimensions.block_size

        assert self.n_block_rows == (self.n_rows + self.block_size - 1) // self.block_size
        assert self.n_block_cols == (self.n_cols + self.block_size - 1) // self.block_size

    def count_blocks(self) -> Tuple[int, int]:
        return (self.n_block_cols, self.n_block_rows)

    def transpose(self) -> 'DNDArray':
        return self.T

    @property
    def T(self) -> 'DNDArray':
        m = self.m
        m = m.annotate(block=m.block.T)
        dimensions = m.globals.collect()[0]
        m = m.select_globals(
            x_field=self.y_field,
            y_field=self.x_field,
            n_rows=dimensions.n_cols,
            n_cols=dimensions.n_rows,
            n_block_rows=dimensions.n_block_cols,
            n_block_cols=dimensions.n_block_rows,
            block_size=dimensions.block_size)
        m = m._key_by_assert_sorted(self.y_field, self.x_field)
        return DNDArray(m)

    def __matmul__(self, right: 'DNDArray') -> 'DNDArray':
        left = self
        assert left.block_size == right.block_size
        assert left.n_cols == right.n_rows
        assert left.n_block_cols == right.n_block_rows

        n_rows = left.n_rows
        n_cols = right.n_cols
        block_size = left.block_size

        n_block_rows = left.n_block_rows
        n_block_inner = left.n_block_cols
        n_block_cols = right.n_block_cols
        n_multiplies = n_block_rows * n_block_cols * n_block_inner

        o = hl.utils.range_table(n_multiplies, n_partitions=n_multiplies)
        o = o.key_by(
            x=o.idx // (n_block_cols * n_block_inner),
            y=(o.idx % (n_block_cols * n_block_inner)) // n_block_inner,
            z=o.idx % n_block_inner
        ).select()
        o = o._key_by_assert_sorted('x', 'y', 'z')
        o = o._key_by_assert_sorted('x', 'z', 'y')
        o = o.annotate(left=left.m[o.x, o.z].block)
        o = o._key_by_assert_sorted('z', 'y', 'x')
        o = o.annotate(right=right.m[o.z, o.y].block)
        o = o.annotate(product=o.left @ o.right)

        # FIXME: use ndarray sum / fma
        def ndarray_to_array(ndarray):
            return hl.rbind(
                ndarray.shape[0],
                ndarray.shape[1],
                lambda n_rows, n_cols: hl.range(hl.int(n_rows * n_cols)).map(
                    lambda absolute: o.product[absolute % n_rows, absolute // n_rows]))
        o = o.annotate(shape=o.product.shape,
                       product=ndarray_to_array(o.product))
        o = o._key_by_assert_sorted('x', 'y', 'z')
        o = o._key_by_assert_sorted('x', 'y')

        import hail.methods.misc as misc
        misc.require_key(o, 'collect_by_key')
        import hail.ir as ir

        o = Table(ir.TableAggregateByKey(
            o._tir,
            hl.struct(
                shape=hl.agg.take(o.shape, 1)[0],
                block=hl.agg.array_sum(o.product))._ir))
        # FIXME: need a "from_column_major" or other semantically meaningful
        # name
        o = o.annotate(block=hl.nd.from_column_major(o.block, o.shape))
        o = o.select('block')
        o = o.select_globals(
            x_field='x',
            y_field='y',
            n_rows=n_rows,
            n_cols=n_cols,
            n_block_rows=n_block_rows,
            n_block_cols=n_block_cols,
            block_size=block_size)
        return DNDArray(o)

    def write(self, *args, **kwargs) -> 'DNDArray':
        return self.m.write(*args, **kwargs)

    def checkpoint(self, fname, *, overwrite=False) -> 'DNDArray':
        self.write(fname, _codec_spec=DNDArray.fast_codec_spec, overwrite=overwrite)
        return read(fname)

    def _force_count_blocks(self) -> int:
        return self.m._force_count()

    def show(self, *args, **kwargs):
        return self.m.show(*args, **kwargs)

    def collect(self) -> np.array:
        blocks = self.m.collect()
        result = [[None
                   for _ in range(self.n_block_cols)]
                  for _ in range(self.n_block_rows)]
        for block in blocks:
            result[block[self.x_field]][block[self.y_field]] = block.block

        return np.concatenate(
            [np.concatenate(result[x], axis=1) for x in range(self.n_block_rows)],
            axis=0
        )
