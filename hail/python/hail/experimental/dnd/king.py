import hail as hl
from hail.expr.expressions import matrix_table_source
from hail.utils.java import Env


def king(call_expr, *, block_size=None):
    mt = matrix_table_source('king/call_expr', call_expr)

    is_hom_ref = Env.get_uid()
    is_het = Env.get_uid()
    is_hom_var = Env.get_uid()
    is_defined = Env.get_uid()
    mt = mt.select_entries(**{
        is_hom_ref: hl.float(hl.or_else(call_expr.is_hom_ref(), 0)),
        is_het: hl.float(hl.or_else(call_expr.is_het(), 0)),
        is_hom_var: hl.float(hl.or_else(call_expr.is_hom_var(), 0)),
        is_defined: hl.float(hl.is_defined(call_expr))
    })
    ref = hl.linalg.BlockMatrix.from_entry_expr(mt[is_hom_ref], block_size=block_size)
    het = hl.linalg.BlockMatrix.from_entry_expr(mt[is_het], block_size=block_size)
    var = hl.linalg.BlockMatrix.from_entry_expr(mt[is_hom_var], block_size=block_size)
    defined = hl.linalg.BlockMatrix.from_entry_expr(mt[is_defined], block_size=block_size)
    ref_var = (ref.T @ var).checkpoint(hl.utils.new_temp_file())
    # We need the count of times the pair is AA,aa and aa,AA. da_ref_var is only AA,aa.
    # Transposing da_ref_var gives da_var_ref, i.e. aa,AA.
    #
    # n.b. (REF.T @ VAR).T == (VAR.T @ REF) by laws of matrix multiply
    N_AA_aa = ref_var + ref_var.T
    N_Aa_Aa = (het.T @ het).checkpoint(hl.utils.new_temp_file())
    # We count the times the row individual has a heterozygous genotype and the
    # column individual has any defined genotype at all.
    N_Aa_defined = (het.T @ defined).checkpoint(hl.utils.new_temp_file())

    het_hom_balance = N_Aa_Aa - (2 * N_AA_aa)
    het_hom_balance = het_hom_balance.to_matrix_table_row_major()
    n_hets_for_rows = N_Aa_defined.to_matrix_table_row_major()
    n_hets_for_cols = N_Aa_defined.T.to_matrix_table_row_major()

    kinship_between = het_hom_balance.rename({'element': 'het_hom_balance'})
    kinship_between = kinship_between.annotate_entries(
        n_hets_row=n_hets_for_rows[kinship_between.row_key, kinship_between.col_key].element,
        n_hets_col=n_hets_for_cols[kinship_between.row_key, kinship_between.col_key].element
    )

    col_index_field = Env.get_uid()
    col_key = mt.col_key
    cols = mt.add_col_index(col_index_field).key_cols_by(col_index_field).cols()

    kinship_between = kinship_between.key_cols_by(
        **cols[kinship_between.col_idx].select(*col_key)
    )

    renaming, _ = deduplicate(list(col_key), already_used=set(col_key))
    assert len(renaming) == len(col_key)

    kinship_between = kinship_between.key_rows_by(
        **cols[kinship_between.row_idx].select(*col_key).rename(dict(renaming))
    )

    kinship_between = kinship_between.annotate_entries(
        min_n_hets=hl.min(kinship_between.n_hets_row,
                          kinship_between.n_hets_col)
    )
    return kinship_between.select_entries(
        phi=(
            0.5
        ) + (
            (
                2 * kinship_between.het_hom_balance +
                - kinship_between.n_hets_row
                - kinship_between.n_hets_col
            ) / (
                4 * kinship_between.min_n_hets
            )
        )
    ).select_rows().select_cols().select_globals()
