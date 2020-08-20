import hail as hl
from hail.expr.expressions import matrix_table_source
from hail.utils.java import Env


def king(call_expr, *, block_size=None):
    r"""Compute relatedness estimates between individuals using a KING variant.

    .. include:: ../_templates/req_diploid_gt.rst

    Examples
    --------
    Estimate kinship for every pair of samples.

    >>> kinship = hl.king(dataset.GT)

    Notes
    -----

    The following presentation pervasively uses matrix-style typesetting rather
    than the typesetting of Manichaikul, et. al.

    Let

    - :math:`i` and :math:`j` be two individuals (possibly their same
      individual) in the dataset given by `call_expr`.

    - :math:`N^{Aa}_{i}` be the number of heterozygote genotype force individual
      :math:`i`.

    - :math:`N^{Aa,Aa}_{i,j}` be the number of variants at which both
      individuals have heterozygote genotypes.

    - :math:`N^{AA,aa}_{i,j}` be the number of variants at which the pair of
      individuals have opposing homozygote genotypes.

    - :math:`S_{i,j}` be the set of single-nucleotide variants for which both
      individuals :math:`i` and :math:`j` have a non-missing genotype.

    - :math:`X_{i,s}` be the genotype score matrix. Homozyogus-reference
      genotypes are represented as 0, heterozygous genotypes are represented as
      1, and homozygous-alternate genotypes are represented as 2. The argument
      `call_expr` defines :math:`X_{i,s}`.

    The three counts above, :math:`N^{Aa}`, :math:`N^{Aa,Aa}`, and
    :math:`N^{AA,aa}`, exclude variants where one or both individuals have
    missing genotypes.

    In terms of the symbols above, we can define :math:`d`, the genetic distance
    between two samples. We can interpret :math:`d` as a unnormalized
    measurement of the genetic material not shared identically-by-descent:

    .. math::

       d_{i,j} = \sum_{s \in S_{i,j}}\left(X_{i,s} - X_{j,s}\right)^2

    In the supplement to Manichaikul, et. al, the authors show how to re-express
    the genetic distance above in terms of the three counts of hetero- and
    homo-zygosity by considering the nine possible configurations of a pair of
    genotypes:

    +-------------------------------+----------+----------+----------+
    |:math:`(X_{i,s} - X_{j,s})^2`  |homref    |het       |homalt    |
    +-------------------------------+----------+----------+----------+
    |homref                         |0         |1         |4         |
    +-------------------------------+----------+----------+----------+
    |het                            |1         |0         |1         |
    +-------------------------------+----------+----------+----------+
    |homalt                         |4         |1         |0         |
    +-------------------------------+----------+----------+----------+

    which leads to this expression for genetic distance:

    .. math::

        d_{i,j} = 4 N^{AA,aa}_{i,j}
                  + N^{Aa}_{i}
                  + N^{Aa}_{j}
                  - 2 N^{Aa,Aa}_{i,j}

    The first term, :math:`4 N^{AA,aa}_{i,j}`, accounts for all pairs of
    genotypes with opposing homozygous genotypes. The second and third terms
    accounts for the four cases of one. Unfortunately, they over count the pair
    of heterozygous case. We offset that with the fourth and final term.

    The genetic distance, :math:`d_{i,j}`, ranges between zero and four times
    the number of variants in the dataset. In the supplement to Manichaikul,
    et. al, the authors demonstrate that kinship, :math:`\phi_{i,j}`, between
    two individuals from the same population is related to the expected genetic
    distance at any *one* variant by way of the allele frequency:

    .. math::

        \mathop{\mathbb{E}}_{i,j} (X_{i,s} - X_{j,s})^2 =
            4 p_s (1 - p_s) (1 - 2\phi_{i,j})

    This identity reveals that the quotient of the expected genetic distance and
    the four-trial binomial variance in the allele frequency, represents,
    roughly, the "fraction of genetic material *not* shared
    identically-by-descent":

    .. math::

        \frac{4 N^{AA,aa}_{i,j} + N^{Aa}_{i} + N^{Aa}_{j} - 2 N^{Aa,Aa}_{i,j}}
             {\sum_{s \in S_{i,j}} 4 p_s (1 - p_s)}

    This is complementary to kinship which is "one-half of the fraction of
    genetic material shared identically-by-descent".

    Manichaikul, et. al demosntrate in Section 2.3 that the sum of the variance
    of the allele frequencies,

    .. math::

        \sum_{s \in S_{i, j}} 2 p_s (1 - p_s)

    is, in expectation, proportional to the count of heterozygous genotypes of
    either individual, if both individuals are members of the same population:

    .. math::

        N^{Aa}_{i}

    For individuals from distinct populations, the authors propose replacing the
    count of heteroyzgous genotypes with the average of the two individuals:

    .. math::

        \frac{N^{Aa}_{i} + N^{Aa}_{j}}{2}

    Using the aforementioned equality, we define a normalized genetic distance,
    :math:`\widetwidle{d_{i,j}}`, for a pair of individuals from distinct
    populations:

    .. math::

        \begin{aligned}
        \widetilde{d_{i,j}} &=
            \frac{4 N^{AA,aa}_{i,j} + N^{Aa}_{i} + N^{Aa}_{j} - 2 N^{Aa,Aa}_{i,j}}
                 {N^{Aa}_{i} + N^{Aa}_{j}} \\
            &= 1
               + \frac{2 N^{AA,aa}_{i,j} - N^{Aa,Aa}_{i,j}}
                      {N^{Aa}_{i} + N^{Aa}_{j}}
        \end{aligned}

    The complement of this normalized genetic distance is called the
    "coefficient of relationship" and is defined as the fraction of genetic
    material shared by two individuals:

    .. math::

        r_{i,j} = 1 - \widetilde{d_{i,j}}

    We now present the KING "within-family" estimator of kinship as one-half of
    the coefficient of relationship:

    .. math::

        \begin{aligned}
        \widehat{\phi_{i,j}^{\prime}} &= \frac{1}{2} r_{i,j} \\
            &= \frac{1}{2} \left( 1 - \widetilde{d_{i,j}} \right) \\
            &= \frac{N^{Aa,Aa}_{i,j} - 2 N^{AA,aa}_{i,j}}
                    {N^{Aa}_{i} + N^{Aa}_{j}}

    This "within-family" estimator over-estimates the kinship coefficient under
    certain circumstances detailed in Section 2.3 of Manichaikul, et. al. The
    authors recommend an alternative estimator when individuals are known to be
    from different families. The estimator replaces the average count of
    heteroyzgous genotypes with the minimum count of heterozygous genotypes:

    .. math::

        \frac{N^{Aa}_{i} + N^{Aa}_{j}}{2} \rightsquigarrow \mathrm{min}(N^{Aa}_{i}, N^{Aa}_{j})

    With this replacement the "within-family" estimator becomes the
    "between-family" estimator, defined in Equation 11 of Manichaikul, et. al.:

    .. math::

        \begin{aligned}
        \widehat{\phi_{i,j}} &=
            \frac{1}{2}
            + \frac{N^{Aa,Aa}_{i,j} - 2 N^{AA,aa}_{i,j}}
                   {2 \cdot \mathrm{min}(N^{Aa}_{i}, N^{Aa}_{j})}
            - \frac{N^{Aa}_{i} + N^{Aa}_{j}}
                   {4 \cdot \mathrm{min}(N^{Aa}_{i}, N^{Aa}_{j})} \\
        \end{aligned}

    This function, :func:`.king`, only implements the "between-family"
    estimator, :math:`\widehat{\phi_{i,j}}`.

    Parameters
    ----------
    call_expr : :class:`.CallExpression`
        Entry-indexed call expression.
    block_size : :obj:`int`, optional
        Block size of block matrices used in the algorithm.
        Default given by :meth:`.BlockMatrix.default_block_size`.

    Returns
    -------
    :class:`.MatrixTable`
        A :class:`.MatrixTable` whose rows and columns are keys are taken from
       `call-expr`'s column keys. It has one entry field, `phi`.
    """
    mt = matrix_table_source('king/call_expr', call_expr)

    n_hets = mt.annotate_cols(n_hets=hl.agg.count_where(call_expr.is_het())).cols()

    is_hom_ref = Env.get_uid()
    is_het = Env.get_uid()
    is_hom_var = Env.get_uid()
    mt = mt.select_entries(**{
        is_hom_ref: hl.float(hl.or_else(call_expr.is_hom_ref(), 0)),
        is_het: hl.float(hl.or_else(call_expr.is_het(), 0)),
        is_hom_var: hl.float(hl.or_else(call_expr.is_hom_var(), 0))
    })
    ref = hl.linalg.BlockMatrix.from_entry_expr(mt[is_hom_ref], block_size=block_size)
    het = hl.linalg.BlockMatrix.from_entry_expr(mt[is_het], block_size=block_size)
    var = hl.linalg.BlockMatrix.from_entry_expr(mt[is_hom_var], block_size=block_size)
    ref_var = (ref.T @ var).checkpoint(hl.utils.new_temp_file())
    # We need the count of times the pair is AA,aa and aa,AA. da_ref_var is only AA,aa.
    # Transposing da_ref_var gives da_var_ref, i.e. aa,AA.
    #
    # n.b. (REF.T @ VAR).T == (VAR.T @ REF) by laws of matrix multiply
    N_AA_aa = ref_var + ref_var.T
    N_Aa_Aa = (het.T @ het).checkpoint(hl.utils.new_temp_file())

    het_hom_balance = N_Aa_Aa - (2 * N_AA_aa)
    het_hom_balance = het_hom_balance.to_matrix_table_row_major()
    mt = mt.add_col_index('col_idx').key_cols_by('col_idx')
    het_hom_balance = het_hom_balance.key_cols_by(
        col=mt.cols()[het_hom_balance.col_idx].sample_idx
    )
    het_hom_balance = het_hom_balance.drop('col_idx')
    het_hom_balance = het_hom_balance.key_rows_by(
        row=mt.cols()[het_hom_balance.row_idx].sample_idx
    )

    # Numerator
    king_robust = het_hom_balance.select_entries(
        het_hom_balance=het_hom_balance.element
    )
    # N^i_Aa
    king_robust = king_robust.annotate_rows(
        n_hets_row=n_hets[king_robust.row_key].n_hets
    )
    # N^j_Aa
    king_robust = king_robust.annotate_cols(
        n_hets_col=n_hets[king_robust.col_key].n_hets
    )
    king_robust = king_robust.annotate_entries(
        min_n_hets=hl.min(king_robust.n_hets_row,
                          king_robust.n_hets_col)
    )
    # Equation 11
    return king_robust.select_entries(
        phi=(
            0.5
        ) + (
            (
                2 * king_robust.het_hom_balance +
                king_robust.n_hets_row +
                king_robust.n_hets_col
            ) / (
                4 * king_robust.min_n_hets
            )
        )
    ).select_rows().select_cols().select_globals()
