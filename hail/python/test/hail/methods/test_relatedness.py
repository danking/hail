import os
import unittest
from subprocess import DEVNULL, call as syscall

import hail as hl
import hail.utils as utils
from ..helpers import resource, get_dataset, skip_unless_spark_backend

setUpModule = startTestHailContext
tearDownModule = stopTestHailContext


@skip_unless_spark_backend()
@unittest.skipIf('HAIL_TEST_SKIP_PLINK' in os.environ, 'Skipping tests requiring plink')
def test_ibd(self):
    dataset = get_dataset()

    def plinkify(ds, min=None, max=None):
        vcf = utils.new_temp_file(prefix="plink", extension="vcf")
        plinkpath = utils.new_temp_file(prefix="plink")
        hl.export_vcf(ds, vcf)
        threshold_string = "{} {}".format("--min {}".format(min) if min else "",
                                          "--max {}".format(max) if max else "")

        plink_command = "plink --double-id --allow-extra-chr --vcf {} --genome full --out {} {}" \
            .format(utils.uri_path(vcf),
                    utils.uri_path(plinkpath),
                    threshold_string)
        result_file = utils.uri_path(plinkpath + ".genome")

        syscall(plink_command, shell=True, stdout=DEVNULL, stderr=DEVNULL)

        ### format of .genome file is:
        # _, fid1, iid1, fid2, iid2, rt, ez, z0, z1, z2, pihat, phe,
        # dst, ppc, ratio, ibs0, ibs1, ibs2, homhom, hethet (+ separated)

        ### format of ibd is:
        # i (iid1), j (iid2), ibd: {Z0, Z1, Z2, PI_HAT}, ibs0, ibs1, ibs2
        results = {}
        with open(result_file) as f:
            f.readline()
            for line in f:
                row = line.strip().split()
                results[(row[1], row[3])] = (list(map(float, row[6:10])),
                                             list(map(int, row[14:17])))
        return results

    def compare(ds, min=None, max=None):
        plink_results = plinkify(ds, min, max)
        hail_results = hl.identity_by_descent(ds, min=min, max=max).collect()

        for row in hail_results:
            key = (row.i, row.j)
            self.assertAlmostEqual(plink_results[key][0][0], row.ibd.Z0, places=4)
            self.assertAlmostEqual(plink_results[key][0][1], row.ibd.Z1, places=4)
            self.assertAlmostEqual(plink_results[key][0][2], row.ibd.Z2, places=4)
            self.assertAlmostEqual(plink_results[key][0][3], row.ibd.PI_HAT, places=4)
            self.assertEqual(plink_results[key][1][0], row.ibs0)
            self.assertEqual(plink_results[key][1][1], row.ibs1)
            self.assertEqual(plink_results[key][1][2], row.ibs2)

    compare(dataset)
    compare(dataset, min=0.0, max=1.0)
    dataset = dataset.annotate_rows(dummy_maf=0.01)
    hl.identity_by_descent(dataset, dataset['dummy_maf'], min=0.0, max=1.0)
    hl.identity_by_descent(dataset, hl.float32(dataset['dummy_maf']), min=0.0, max=1.0)


@skip_unless_spark_backend()
def test_pc_relate_against_R_truth(self):
    mt = hl.import_vcf(resource('pc_relate_bn_input.vcf.bgz'))
    hail_kin = hl.pc_relate(mt.GT, 0.00, k=2).checkpoint(utils.new_temp_file(extension='ht'))

    r_kin = hl.import_table(resource('pc_relate_r_truth.tsv.bgz'),
                            types={'i': 'struct{s:str}',
                                   'j': 'struct{s:str}',
                                   'kin': 'float',
                                   'ibd0': 'float',
                                   'ibd1': 'float',
                                   'ibd2': 'float'},
                            key=['i', 'j'])
    assert r_kin.select("kin")._same(hail_kin.select("kin"), tolerance=1e-3, absolute=True)
    assert r_kin.select("ibd0")._same(hail_kin.select("ibd0"), tolerance=1.3e-2, absolute=True)
    assert r_kin.select("ibd1")._same(hail_kin.select("ibd1"), tolerance=2.6e-2, absolute=True)
    assert r_kin.select("ibd2")._same(hail_kin.select("ibd2"), tolerance=1.3e-2, absolute=True)


@skip_unless_spark_backend()
def test_pc_relate_simple_example(self):
    gs = hl.literal(
        [[0, 0, 0, 0, 1, 1, 1, 1],
         [0, 0, 1, 1, 0, 0, 1, 1],
         [0, 1, 0, 1, 0, 1, 0, 1],
         [0, 0, 1, 1, 0, 0, 1, 1]])
    scores = hl.literal([[0, 1], [1, 1], [1, 0], [0, 0]])
    mt = hl.utils.range_matrix_table(n_rows=8, n_cols=4)
    mt = mt.annotate_entries(GT=hl.unphased_diploid_gt_index_call(gs[mt.col_idx][mt.row_idx]))
    mt = mt.annotate_cols(scores=scores[mt.col_idx])
    pcr = hl.pc_relate(mt.GT, min_individual_maf=0, scores_expr=mt.scores)

    expected = [
        hl.Struct(i=0, j=1, kin=-0.14570713364640647,
                  ibd0=1.4823511628401964, ibd1=-0.38187379109476693, ibd2=-0.10047737174542953),
        hl.Struct(i=0, j=2, kin=0.16530591922102378,
                  ibd0=0.5234783206257841, ibd1=0.2918196818643366, ibd2=0.18470199750987923),
        hl.Struct(i=0, j=3, kin=-0.14570713364640647,
                  ibd0=1.4823511628401964, ibd1=-0.38187379109476693, ibd2=-0.10047737174542953),
        hl.Struct(i=1, j=2, kin=-0.14570713364640647,
                  ibd0=1.4823511628401964, ibd1=-0.38187379109476693, ibd2=-0.10047737174542953),
        hl.Struct(i=1, j=3, kin=0.14285714285714285,
                  ibd0=0.7027734170591313, ibd1=0.02302459445316596, ibd2=0.2742019884877027),
        hl.Struct(i=2, j=3, kin=-0.14570713364640647,
                  ibd0=1.4823511628401964, ibd1=-0.38187379109476693, ibd2=-0.10047737174542953),
    ]
    ht_expected = hl.Table.parallelize(expected)
    ht_expected = ht_expected.key_by(i=hl.struct(col_idx=ht_expected.i),
                                     j=hl.struct(col_idx=ht_expected.j))
    assert ht_expected._same(pcr)


@skip_unless_spark_backend()
def test_pcrelate_paths(self):
    mt = hl.balding_nichols_model(3, 50, 100)
    _, scores3, _ = hl.hwe_normalized_pca(mt.GT, k=3, compute_loadings=False)

    kin1 = hl.pc_relate(mt.GT, 0.10, k=2, statistics='kin', block_size=64)
    kin2 = hl.pc_relate(mt.GT, 0.05, k=2, min_kinship=0.01, statistics='kin2', block_size=128).cache()
    kin3 = hl.pc_relate(mt.GT, 0.02, k=3, min_kinship=0.1, statistics='kin20', block_size=64).cache()
    kin_s1 = hl.pc_relate(mt.GT, 0.10, scores_expr=scores3[mt.col_key].scores[:2],
                          statistics='kin', block_size=32)

    assert kin1._same(kin_s1, tolerance=1e-4)

    assert kin1.count() == 50 * 49 / 2

    assert kin2.count() > 0
    assert kin2.filter(kin2.kin < 0.01).count() == 0

    assert kin3.count() > 0
    assert kin3.filter(kin3.kin < 0.1).count() == 0


@skip_unless_spark_backend()
def test_pcrelate_issue_5263(self):
    mt = hl.balding_nichols_model(3, 50, 100)
    expected = hl.pc_relate(mt.GT, 0.10, k=2, statistics='all')
    mt = mt.select_entries(GT2=mt.GT,
                           GT=hl.call(hl.rand_bool(0.5), hl.rand_bool(0.5)))
    actual = hl.pc_relate(mt.GT2, 0.10, k=2, statistics='all')
    assert expected._same(actual, tolerance=1e-4)


def test_king_small(self):
    
