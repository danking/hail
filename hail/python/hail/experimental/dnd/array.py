import json
import numpy as np
from typing import Optional, Tuple, Callable, Any
import tabulate

from hail.utils.java import Env, info
from hail.utils import range_table, new_temp_file
from hail.expr import Expression
from hail import nd
from hail.matrixtable import MatrixTable
from hail.table import Table

import hail as hl


def array(mt: MatrixTable,
          entry_field: str,
          *,
          n_partitions: Optional[int] = None,
          block_size: Optional[int] = None,
          sort_columns: bool = False) -> 'DNDArray':
    return DNDArray.from_matrix_table(mt,
                                      entry_field,
                                      n_partitions=n_partitions,
                                      block_size=block_size,
                                      sort_columns=sort_columns)


def read(fname: str) -> 'DNDArray':
    # read without good partitioning, just to get the globals
    a = DNDArray(hl.read_table(fname + '.m'),
                 hl.read_table(fname + '.row_fields'),
                 hl.read_table(fname + '.col_fields'))
    t = hl.read_table(fname + '.m', _intervals=[
        hl.Interval(hl.Struct(r=i, c=j),
                    hl.Struct(r=i, c=j + 1))
        for i in range(a.n_block_rows)
        for j in range(a.n_block_cols)])
    return DNDArray(t,
                    hl.read_table(fname + '.row_fields'),
                    hl.read_table(fname + '.col_fields'))


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
            block_size: Optional[int] = None,
            sort_columns: bool = False
    ) -> 'DNDArray':
        if n_partitions is None:
            n_partitions = mt.n_partitions()
        if block_size is None:
            block_size = DNDArray.default_block_size
        if n_partitions == 0:
            assert mt.count_cols() == 0
            assert mt.count_rows() == 0
            t = range_table(0, 0)
            t = t.annotate(r=0, c=0, block=nd.array([]).reshape((0, 0)))
            t = t.select_globals(
                n_rows=0,
                n_cols=0,
                n_block_rows=0,
                n_block_cols=0,
                block_size=0)
            return DNDArray(t, t, t)

        assert 'r' not in mt.row
        assert 'c' not in mt.row
        assert 'block' not in mt.row

        n_rows, n_cols = mt.count()
        n_block_rows = (n_rows + block_size - 1) // block_size
        n_block_cols = (n_cols + block_size - 1) // block_size
        entries, cols, row_index, col_blocks = (Env.get_uid() for _ in range(4))

        if sort_columns:
            col_index = Env.get_uid()
            col_order = mt.add_col_index(col_index)
            col_order = col_order.key_cols_by().cols()
            col_order = col_order.select(key=col_order.row.select(*mt.col_key),
                                         index=col_order[col_index])
            col_order = col_order.collect(_localize=False)
            col_order = hl.sorted(col_order, key=lambda x: x.key)
            col_order = col_order['index'].collect()[0]
            mt = mt.choose_cols(col_order)
        else:
            col_keys = mt.col_key.collect(_localize=False)
            out_of_order = hl.range(hl.len(col_keys) - 1).map(
                lambda i: col_keys[i] > col_keys[i+1])
            out_of_order = out_of_order.collect()[0]
            if any(out_of_order):
                raise ValueError(
                    f'from_matrix_table: columns are not in sorted order. You may request a '
                    f'sort with sort_columns=True.')

        row_key = list(mt.row_key)

        local_col_fields = mt.key_cols_by().cols().key_by(*mt.col_key)  # silence the warning about ordering changes
        local_col_fields = local_col_fields.collect(_localize=False)
        local_col_fields = hl.range(n_block_cols).flatmap(
            lambda block_col: local_col_fields[(block_col * block_size):
                                               ((block_col + 1) * block_size)].map(
                lambda col_key: hl.struct(c=block_col, **col_key)))
        col_fields = Table.parallelize(local_col_fields)
        col_fields = col_fields.key_by('c', *mt.col_key)

        mt = (mt
              .select_globals()
              .select_rows()
              .select_cols()
              .add_row_index(row_index)
              .localize_entries(entries, cols))
        mt = mt.annotate(r=hl.int(mt[row_index] // block_size))

        row_fields = mt.select('r')
        row_fields = row_fields.key_by('r', *row_key)

        # FIXME: remove when ndarray support structs
        mt = mt.annotate(**{entries: mt[entries][entry_field]})
        mt = mt.annotate(
            **{col_blocks: hl.range(n_block_cols).map(
                lambda c: hl.struct(
                    c=c,
                    entries=mt[entries][(c * block_size):((c + 1) * block_size)]))}
        )
        mt = mt.explode(col_blocks)
        mt = mt.select(row_index, 'r', **mt[col_blocks])
        mt = mt.key_by(mt.r, mt.c)
        mt = mt.group_by(mt.r, mt.c).aggregate(
            entries=hl.sorted(
                hl.agg.collect(hl.struct(row_index=mt[row_index], entries=mt.entries)),
                key=lambda x: x.row_index
            ).map(lambda x: x.entries))
        mt = mt.select(block=hl.nd.array(mt.entries))
        mt = mt.select_globals(
            n_rows=n_rows,
            n_cols=n_cols,
            n_block_rows=n_block_rows,
            n_block_cols=n_block_cols,
            block_size=block_size)
        fname = new_temp_file()
        mt = mt.key_by(mt.r, mt.c)
        mt.write(fname, _codec_spec=DNDArray.fast_codec_spec)
        t = hl.read_table(fname, _intervals=[
            hl.Interval(hl.Struct(r=i, c=j),
                        hl.Struct(r=i, c=j + 1))
            for i in range(n_block_rows)
            for j in range(n_block_cols)])
        return DNDArray(t, row_fields, col_fields)

    def __init__(self, t: Table, row_fields: Table, col_fields: Table) -> 'DNDArray':
        dimensions = t.globals.collect()[0]
        self.n_rows: int = dimensions.n_rows
        self.n_cols: int = dimensions.n_cols
        self.n_block_rows: int = dimensions.n_block_rows
        self.n_block_cols: int = dimensions.n_block_cols
        self.block_size: int = dimensions.block_size

        assert 'r' in t.row
        assert 'c' in t.row
        assert 'block' in t.row

        assert 'r' in row_fields.row

        assert 'c' in col_fields.row

        self.m: Table = t
        self.row_fields: Table = row_fields
        self.col_fields: Table = col_fields

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
            n_rows=dimensions.n_cols,
            n_cols=dimensions.n_rows,
            n_block_rows=dimensions.n_block_cols,
            n_block_cols=dimensions.n_block_rows,
            block_size=dimensions.block_size)
        m = m._key_by_assert_sorted('c', 'r')
        m = m.rename({'r': 'c', 'c': 'r'})
        col_fields = self.col_fields.rename({'c': 'r'})
        row_fields = self.row_fields.rename({'r': 'c'})
        return DNDArray(m, col_fields, row_fields)

    def _block_inner_product(self,
                             right: 'DNDArray',
                             block_product: Callable[[Expression, Expression], Expression],
                             block_aggregate: Callable[[Expression], Expression]
                             ) -> 'DNDArray':
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
            r=o.idx // (n_block_cols * n_block_inner),
            c=(o.idx % (n_block_cols * n_block_inner)) // n_block_inner,
            k=o.idx % n_block_inner
        ).select()
        o = o._key_by_assert_sorted('r', 'c', 'k')
        o = o._key_by_assert_sorted('r', 'k', 'c')
        o = o.annotate(left=left.m[o.r, o.k].block)
        o = o._key_by_assert_sorted('k', 'c', 'r')
        o = o.annotate(right=right.m[o.k, o.c].block)

        o = o.annotate(product=block_product(o.left, o.right))
        o = o._key_by_assert_sorted('r', 'c', 'k')
        o = o._key_by_assert_sorted('r', 'c')

        import hail.methods.misc as misc
        misc.require_key(o, 'collect_by_key')
        import hail.ir as ir

        o = Table(ir.TableAggregateByKey(
            o._tir,
            hl.struct(block=block_aggregate(o.product))._ir))
        o = o.select('block')
        o = o.select_globals(
            n_rows=n_rows,
            n_cols=n_cols,
            n_block_rows=n_block_rows,
            n_block_cols=n_block_cols,
            block_size=block_size)
        return DNDArray(o, left.row_fields, right.col_fields)

    def __matmul__(self, right: 'DNDArray') -> 'DNDArray':
        return self._block_inner_product(right,
                                         lambda left, right: left @ right,
                                         hl.agg.ndarray_sum)

    def inner_product(self,
                      right: 'DNDArray',
                      multiply: Callable[[Expression, Expression], Expression],
                      add: Callable[[Expression, Expression], Expression],
                      zero: Expression,
                      add_as_an_aggregator: Callable[[Expression], Expression]
                      ) -> 'DNDArray':
        def block_product(left, right):
            n_rows, n_inner = left.shape
            _, n_cols = right.shape

            def compute_element(absolute):
                return hl.rbind(
                    absolute % n_rows,
                    absolute // n_rows,
                    lambda row, col: hl.range(hl.int(n_inner)).map(
                        lambda inner: multiply(left[row, inner], right[inner, col])
                    ).fold(add, zero))

            return hl.struct(
                shape=(left.shape[0], right.shape[1]),
                block=hl.range(hl.int(n_rows * n_cols)).map(compute_element))

        def block_aggregate(prod):
            shape = prod.shape
            block = prod.block
            return hl.nd.from_column_major(
                hl.agg.array_agg(add_as_an_aggregator, block),
                hl.agg.take(shape, 1)[0])

        return self._block_inner_product(right, block_product, block_aggregate)

    def _block_map(self, fun: Callable[[Expression], Expression]) -> 'DNDArray':
        o = self.m
        o = o.annotate(block=fun(o.block))
        return DNDArray(o, self.row_fields, self.col_fields)

    def _block_pairwise_map(self,
                            right: 'DNDArray',
                            fun: Callable[[Expression, Expression], Expression]
                            ) -> 'DNDArray':
        left = self
        assert left.block_size == right.block_size
        assert left.n_rows == right.n_rows
        assert left.n_cols == right.n_cols
        assert left.n_block_rows == right.n_block_rows
        assert left.n_block_cols == right.n_block_cols

        o = left.m
        o = o.annotate(
            block=fun(o.block, right.m[o.r, o.c].block))

        row_fields = left.row_fields
        row_fields = row_fields.annotate(
            right=right.row_fields[row_fields.key])

        col_fields = left.col_fields
        col_fields = col_fields.annotate(
            right=right.col_fields[col_fields.key])

        return DNDArray(o, row_fields, col_fields)

    def __add__(self, right: Any) -> 'DNDArray':
        assert not isinstance(right, Table)
        assert not isinstance(right, MatrixTable)

        if not isinstance(right, DNDArray):
            return self._block_map(lambda left: left + right)

        return self._block_pairwise_map(right, lambda left, right: left + right)

    def __radd__(self, left: Any) -> 'DNDArray':
        assert not isinstance(left, Table)
        assert not isinstance(left, MatrixTable)
        assert not isinstance(left, DNDArray)

        return self._block_map(lambda right: left + right)

    def __sub__(self, right: Any) -> 'DNDArray':
        assert not isinstance(right, Table)
        assert not isinstance(right, MatrixTable)

        if not isinstance(right, DNDArray):
            return self._block_map(lambda left: left - right)

        return self._block_pairwise_map(right, lambda left, right: left - right)

    def __rsub__(self, left: Any) -> 'DNDArray':
        assert not isinstance(left, Table)
        assert not isinstance(left, MatrixTable)
        assert not isinstance(left, DNDArray)

        return self._block_map(lambda right: left - right)

    def __mul__(self, right: Any) -> 'DNDArray':
        assert not isinstance(right, Table)
        assert not isinstance(right, MatrixTable)

        if not isinstance(right, DNDArray):
            return self._block_map(lambda left: left * right)

        return self._block_pairwise_map(right, lambda left, right: left * right)

    def __rmul__(self, left: Any) -> 'DNDArray':
        assert not isinstance(left, Table)
        assert not isinstance(left, MatrixTable)
        assert not isinstance(left, DNDArray)

        return self._block_map(lambda right: left * right)

    def write(self, fname, *, _codec_spec=None, overwrite=False) -> 'DNDArray':
        if _codec_spec is None:
            _codec_spec = DNDArray.fast_codec_spec
        kwargs = {'_codec_spec': _codec_spec,
                  'overwrite': overwrite}
        # FIXME: write a directory of things
        self.m.write(fname + '.m', **kwargs)
        self.row_fields.write(fname + '.row_fields', **kwargs)
        self.col_fields.write(fname + '.col_fields', **kwargs)

    def checkpoint(self, fname, *, overwrite=False) -> 'DNDArray':
        self.write(fname, _codec_spec=DNDArray.fast_codec_spec, overwrite=overwrite)
        return read(fname)

    def _force_count_blocks(self) -> int:
        return self.m._force_count()

    def show(self,
             n_rows=None,
             n_cols=None,
             # FIXME: support these
             # width=None,
             # truncate=None,
             types=True,
             handler=None):
        if handler is None:
            handler = hl.utils.default_handler()
        handler(ShowDNDArray(self, n_rows, n_cols, types))

    def collect(self) -> np.array:
        blocks = self.m.collect()
        result = [[None
                   for _ in range(self.n_block_cols)]
                  for _ in range(self.n_block_rows)]
        for block in blocks:
            result[block.r][block.c] = block.block

        return np.concatenate(
            [np.concatenate(result[x], axis=1) for x in range(self.n_block_rows)],
            axis=0
        )

    def to_matrix_table(self) -> MatrixTable:
        m = self.m
        m = m._key_by_assert_sorted('r')


        import hail.methods.misc as misc
        misc.require_key(m, 'collect_by_key')
        import hail.ir as ir

        col_fields = self.col_fields
        row_field_names = set(self.row_fields.row)
        renames = {}
        for col in list(col_fields.row):
            if col != 'c':
                new_col = col
                i = 0
                while new_col in row_field_names:
                    new_col = new_col + f'_{i}'
                renames[new_col] = col
        # FIXME: not always renames, filter to the real renames
        info('DNDArray.to_matrix_table: renamed the following column fields to avoid name conflicts:'
             + ''.join(f'\n    {repr(old)} -> {repr(new)}' for new, old in renames.items()))

        cols = col_fields.collect(_localize=False).map(
            lambda col: col.select(**{new: col[old]
                                      for new, old in renames.items()}))

        # FIXME: use ndarray hcat
        def ndarray_to_array(arr):
            n_rows = arr.shape[0]
            n_cols = arr.shape[1]
            return hl.range(hl.int(n_rows)).map(
                lambda r: hl.range(hl.int(n_cols)).map(
                    lambda c: arr[r, c]))
        m = Table(ir.TableAggregateByKey(
            m._tir,
            hl.struct(
                blocks=hl.sorted(
                    hl.agg.collect(hl.struct(c=m.c, block=ndarray_to_array(m.block))),
                    key=lambda x: x.c
                ).map(lambda x: x.block))._ir))
        row_fields_by_r_only = self.row_fields.key_by('r')
        m = m.annotate(
            row_fields=row_fields_by_r_only.index(m.r, all_matches=True)
        )
        m = m.annotate(
            entries=hl.range(self.block_size).map(
                lambda r: hl.flatten(m.blocks.map(lambda block: block[r])))
        )
        m = m.select(
            entries_and_keys=hl.zip(m.row_fields, m.entries)
        )
        m = m.explode('entries_and_keys')
        m = m.annotate_globals(cols=cols)
        m = m.key_by()
        key_fields = list(self.row_fields.key[1:])
        m = m.select(
            **m.entries_and_keys[0],
            entries=m.entries_and_keys[1].map(lambda x: hl.struct(x=x))
        )
        m = m.key_by(*key_fields)
        col_key = [new for new, old in renames.items()
                   if old in self.col_fields.key[1:]]
        m = m._unlocalize_entries('entries', 'cols', col_key)
        m = m.select_globals()
        m = m.checkpoint(new_temp_file())
        return m


class ShowDNDArray:
    def __init__(self, da, n_rows, n_cols, types):
        self.da = da
        # FIXME: get row count form terminal or something?
        self.n_rows = n_rows or 10
        self.n_block_rows = (self.n_rows + self.da.block_size - 1) // self.da.block_size
        self.n_cols = n_cols or 10
        self.n_block_cols = (self.n_cols + self.da.block_size - 1) // self.da.block_size
        self.types = types
        self._row_data = None
        self._col_data = None
        self._entry_data = None

    def __str__(self):
        return self._ascii_str()

    def __repr__(self):
        return self.__str__()

    def _get_data(self):
        if self._row_data is None:
            row_fields = self.da.row_fields.flatten().drop('r')
            row_dtype = row_fields.row.dtype
            row_fields = row_fields.select(**{
                k: hl._showstr(v) for (k, v) in row_fields.row.items()})
            rows, rows_has_more = row_fields._take_n(self.n_rows)
            self._row_data = (rows, rows_has_more, row_dtype)

            col_fields = self.da.col_fields.flatten().drop('c')
            col_dtype = col_fields.row.dtype
            col_fields = col_fields.select(**{
                k: hl._showstr(v) for (k, v) in col_fields.row.items()})
            cols, cols_has_more = col_fields._take_n(self.n_cols)
            self._col_data = (cols, cols_has_more, col_dtype)

            m = self.da.m
            m = m.filter((m.r <= self.n_block_rows) & (m.c <= self.n_block_cols))
            blocks = m.collect()
            n_cols = min(self.n_cols, self.da.n_cols)
            n_rows = min(self.n_rows, self.da.n_rows)
            matrix = [[None for _ in range(n_cols)] for _ in range(n_rows)]
            # FIXME: should really use showstr, can ndarray have string inside?
            for block in blocks:
                block_row = block.r
                block_col = block.c
                block = block.block
                for row_in_block in range(self.da.block_size):
                    row = row_in_block + block_row * self.da.block_size
                    if row >= n_rows:
                        break
                    for col_in_block in range(self.da.block_size):
                        col = col_in_block + block_col * self.da.block_size
                        if col >= n_cols:
                            break
                        matrix[row][col] = block[row_in_block, col_in_block]
            self._entry_data = (matrix, m.block.dtype.element_type)

    def row_data(self):
        self._get_data()
        return self._row_data

    def col_data(self):
        self._get_data()
        return self._col_data

    def entry_data(self):
        self._get_data()
        return self._entry_data

    def _repr_html_(self):
        return self._html_str()

    def _ascii_str(self):
        rows, has_more_rows, row_dtype = self.row_data()
        row_fields = list(row_dtype)

        # type_strs = [str(dtype[f]) for f in fields] if types else [''] * len(fields)
        # right_align = [hl.expr.types.is_numeric(dtype[f]) for f in fields]

        rows = [[row[f] for f in row_fields] for row in rows]

        cols, has_more_cols, col_dtype = self.col_data()
        if len(col_dtype) == 1:
            cols = [c[list(c)[0]] for c in cols]
        entries, entry_dtype = self.entry_data()

        row_field_headers = list(row_dtype)
        entry_field_headers = list(cols)
        if self.types:
            row_field_headers = [f'{field}\n{row_dtype[field]}' for field in row_field_headers]
            entry_field_headers = [f'{col}\n{entry_dtype}' for col in entry_field_headers]

        table_header = row_field_headers + entry_field_headers
        table_rows = [row + entry_vector for row, entry_vector in zip(rows, entries)]

        table_string = tabulate.tabulate(
            table_rows, headers=table_header, tablefmt='psql', numalign='right') + '\n'

        if has_more_rows:
            n_rows = len(rows)
            table_string += f"showing top { n_rows } { 'row' if n_rows == 1 else 'rows' }\n"
        if has_more_cols:
            n_cols = len(cols)
            table_string += f"showing top { n_cols } { 'col' if n_cols == 1 else 'cols' }\n"
        return table_string

    def _html_str(self):
        import html
        rows, has_more_rows, row_dtype = self.row_data()
        row_fields = list(row_dtype)

        # type_strs = [str(dtype[f]) for f in fields] if types else [''] * len(fields)
        # right_align = [hl.expr.types.is_numeric(dtype[f]) for f in fields]

        rows = [[row[f] for f in row_fields] for row in rows]

        cols, has_more_cols, col_dtype = self.col_data()
        if len(col_dtype) == 1:
            cols = [c[list(c)[0]] for c in cols]
        entries, entry_dtype = self.entry_data()

        if self.types:
            row_field_headers = [
                f'{html.escape(str(f))}<br/>{html.escape(str(row_dtype[f]))}'
                for f in row_dtype]
            entry_field_headers = [
                f'{html.escape(str(c))}<br/>{html.escape(str(entry_dtype))}'
                for c in cols]
        else:
            row_field_headers = [html.escape(str(field)) for field in row_dtype]
            entry_field_headers = [html.escape(str(col)) for col in cols]

        table_header = row_field_headers + entry_field_headers
        table_rows = [row + entry_vector for row, entry_vector in zip(rows, entries)]

        table_string = tabulate.tabulate(
            table_rows, headers=table_header, tablefmt='html', numalign='right')

        if has_more_rows:
            n_rows = len(rows)
            table_string += f'<p style="background: #fdd; padding: 0.4em;">showing top { n_rows } { "row" if n_rows == 1 else "rows" }</p>\n'
        if has_more_cols:
            n_cols = len(cols)
            table_string += f'<p style="background: #fdd; padding: 0.4em;">showing top { n_cols } { "col" if n_cols == 1 else "cols" }</p>\n'

        css = '''
<style>
.hail_dnd_array td {
    white-space: nowrap;
    max-width: 500px;
    overflow: hidden;
    text-overflow: ellipsis;
    font-weight: normal;
}
</style>
        '''
        return css + '<div class="hail_dnd_array">' + table_string + '</div>'
