from time import sleep
from codecs import decode

import py_wirbelwind
from gcp.path import parse
from gcp.storage import download_blob
from jacquard_operations.production_environment import get_execution_context_from_anywhere
from py_wirbelwind import EdgehillType
from py_wirbelwind.cellranger import FEATURE_REF_PATH_LEGACY_DEPRECATED, NamedCellrangerDistribution
from py_wirbelwind.longbow import get_longbow_run_outputs
from py_wirbelwind.submission import create_label_builder


def _fetch_panel(gs_path: str) -> str:
    return decode(download_blob(parse(gs_path)).read(), 'utf-8')


if __name__ == "__main__":
    # Needs SSH tunneling on port 8000 to Cromwell
    with get_execution_context_from_anywhere() as execution_context:
        api = execution_context.wirbelwind_executor
        for job in [
            # py_wirbelwind.longbow.create_longbow_configuration(
            #     labels=create_label_builder('example edgehill+longbow feature7'),
            #     internal_id="foo",
            #     feature_glob="gs://adelaide-ad-hoc-datasets/sequencing-fastq/20200114-genewiz-30-328548137/Feature7*",
            #     gex_glob="gs://adelaide-ad-hoc-datasets/sequencing-fastq/20200114-genewiz-30-328548137/GEX/GEX7*",
            #     crispr_glob='',
            #     feature_ref_content=_fetch_panel("20200114-genewiz-30-328548137/feature_barcode.csv"),
            #     cellranger_distribution=NamedCellrangerDistribution.cellranger4.value,
            #     expected_cell_count=25000,
            # ),
            # py_wirbelwind.longbow.create_longbow_configuration(
            #     labels=create_label_builder('example edgehill+longbow cellranger3 longbow'),
            #     internal_id="foo3",
            #     feature_glob="gs://augustus-automatically-managed/sequencing_fastq/30-358457518_fastq_v1/Feature1*",
            #     gex_glob="gs://augustus-automatically-managed/sequencing_fastq/30-358457518_fastq_v1/GEX1*",
            #     crispr_glob='',
            #     feature_ref_content=_fetch_panel(FEATURE_REF_PATH_LEGACY_DEPRECATED),
            #     cellranger_distribution=NamedCellrangerDistribution.cellranger3.value,
            #     expected_cell_count=25000,
            # ),
            # py_wirbelwind.longbow.create_longbow_configuration(
            #     labels=create_label_builder('example edgehill+longbow cellranger4 longbow'),
            #     internal_id="foo4",
            #     feature_glob="gs://augustus-automatically-managed/sequencing_fastq/30-358457518_fastq_v1/Feature1*",
            #     gex_glob="gs://augustus-automatically-managed/sequencing_fastq/30-358457518_fastq_v1/GEX1*",
            #     crispr_glob='',
            #     feature_ref_content=_fetch_panel(FEATURE_REF_PATH_LEGACY_DEPRECATED),
            #     expected_cell_count=25000,
            #     cellranger_distribution=NamedCellrangerDistribution.cellranger3.value,
            # ),
            # py_wirbelwind.edgehill.create_edgehill_configuration(
            #     labels=create_label_builder('example edgehill+longbow cellranger4 count tcr'),
            #     run_type=EdgehillType.COUNT,
            #     input_dir='gs://augustus-automatically-managed/sequencing_fastq/30-358457518_fastq_v1/TCR1*',
            #     expected_cell_count=5000,
            #     cellranger_distribution=NamedCellrangerDistribution.cellranger4.value,
            # ),
            # py_wirbelwind.edgehill.create_edgehill_configuration(
            #     labels=create_label_builder('example edgehill+longbow cellranger3 count tcr'),
            #     run_type=EdgehillType.COUNT,
            #     input_dir='gs://augustus-automatically-managed/sequencing_fastq/30-358457518_fastq_v1/TCR1*',
            #     expected_cell_count=5000,
            #     cellranger_distribution=NamedCellrangerDistribution.cellranger3.value,
            # ),
            # py_wirbelwind.edgehill.create_edgehill_configuration(
            #     labels=create_label_builder('example edgehill+longbow cellranger3 tcr'),
            #     run_type=EdgehillType.VDJ,
            #     input_dir='gs://augustus-automatically-managed/sequencing_fastq/30-358457518_fastq_v1/TCR1*',
            #     expected_cell_count=5000,
            #     cellranger_distribution=NamedCellrangerDistribution.cellranger4.value,
            # ),
            py_wirbelwind.edgehill.create_edgehill_configuration(
                labels=create_label_builder('example edgehill+longbow cellranger4 tcr'),
                run_type=EdgehillType.VDJ,
                input_dir='gs://scaramouche_sandbox/chenling/test1/',
                cellranger_distribution=NamedCellrangerDistribution.cellranger4.value,
            ),
            # py_wirbelwind.edgehill.create_edgehill_configuration(
            #     labels=create_label_builder('example edgehill+longbow atac'),
            #     run_type=EdgehillType.ATAC_COUNT,
            #     input_dir='gs://adelaide-ad-hoc-datasets/sequencing-fastq/20200106-genewiz-30-326562823/B5732pitstop-*',
            #     expected_cell_count=5000,
            #     cellranger_distribution=NamedCellrangerDistribution.cellranger3_atac.value,
            # ),
        ]:
            result = api.execute_job(job)
            print(result)

            delay = 15
            print(f"Sleeping {delay} seconds, will not get a status immediately")
            sleep(delay)
            print(f"Status {api.get_job_status(result.id)}")

            # api.abort(result.id)
            # print("If we made it here, abort was successful. No boom, commander!")

    details = api.get_metadata("a3557b3d-9689-47d5-8f95-fc4fa9c4dd9e")  # pylint: disable=invalid-name
    print(get_longbow_run_outputs(details))
