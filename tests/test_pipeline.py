from os.path import join

from freezegun import freeze_time
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.cbpf.pipeline import Pipeline

EXPECTED_DATASET = {
    "name": "cbpf-allocations-and-contributions",
    "title": "Global - CBPF Allocations and Contributions",
    "dataset_date": "[2014-02-10T00:00:00 TO 2014-06-03T23:59:59]",
    "tags": [
        {"name": "funding", "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1"},
        {
            "name": "humanitarian response plan-hrp",
            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
        },
    ],
    "groups": [{"name": "world"}],
    "license_id": "cc-by",
    "methodology": "Registry",
    "caveats": "Although the stated expected update frequency is monthly, updates to the data may occur more or less frequently.",
    "dataset_source": "CBPF",
    "package_creator": "HDX Data Systems Team",
    "private": False,
    "maintainer": "d1e11ac4-8fa2-485a-8e16-d5bd90aee1a0",
    "owner_org": "75b74751-1b97-4cde-939b-1e4fb083c85c",
    "data_update_frequency": 30,
    "notes": "This dataset contains approved project allocations from Country-Based Pooled Funds (CBPFs) and the contributions received by each fund.",
}

EXPECTED_RESOURCES = [
    {
        "name": "global_cbpf_contributions.csv",
        "description": "This csv contains all the contributions in the pooled funds system.",
        "format": "csv",
    },
    {
        "name": "global_cbpf_project_summary.csv",
        "description": "This csv contains the project summaries for all approved projects.",
        "format": "csv",
    },
]

EXPECTED_COUNTRY_DATASET = {
    "name": "afghanistan-cbpf-allocations-and-contributions",
    "title": "Afghanistan - CBPF Allocations and Contributions",
    "dataset_date": "[2014-02-10T00:00:00 TO 2014-06-03T23:59:59]",
    "tags": [
        {"name": "funding", "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1"},
        {
            "name": "humanitarian response plan-hrp",
            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
        },
    ],
    "groups": [{"name": "afg"}],
    "license_id": "cc-by",
    "methodology": "Registry",
    "caveats": "Although the stated expected update frequency is monthly, updates to the data may occur more or less frequently.",
    "dataset_source": "CBPF",
    "package_creator": "HDX Data Systems Team",
    "private": False,
    "maintainer": "d1e11ac4-8fa2-485a-8e16-d5bd90aee1a0",
    "owner_org": "75b74751-1b97-4cde-939b-1e4fb083c85c",
    "data_update_frequency": 30,
    "notes": "This dataset contains approved project allocations from Country-Based Pooled Funds (CBPFs) and the contributions received by each fund for Afghanistan.",
}

EXPECTED_COUNTRY_RESOURCES = [
    {
        "name": "afg_cbpf_contributions.csv",
        "description": "This csv contains all the contributions for Afghanistan in the pooled funds system. See live data from the CBPF API for Afghanistan [here](https://cbpfapi.unocha.org/vo1/odata/Contribution?poolfundAbbrv=AFG23).",
        "format": "csv",
    },
    {
        "name": "afg_cbpf_project_summary.csv",
        "description": "This csv contains the project summaries for all approved projects in Afghanistan. See live data from the CBPF API for Afghanistan [here](https://cbpfapi.unocha.org/vo1/odata/ProjectSummary?poolfundAbbrv=AFG23).",
        "format": "csv",
    },
]


@freeze_time("2026-04-30")
class TestPipeline:
    def test_pipeline(
        self,
        configuration,
        fixtures_dir,
        input_dir,
        config_dir,
    ):
        with temp_dir(
            "TestCbpf",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)
                datasets = pipeline.generate_datasets(existing_resource_ids={})
                assert len(datasets) == 2

                global_dataset = datasets[0]
                global_dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert global_dataset == EXPECTED_DATASET
                global_resources = global_dataset.get_resources()
                assert global_resources == EXPECTED_RESOURCES
                for resource in global_resources:
                    assert_files_same(
                        join(tempdir, resource["name"]),
                        join(input_dir, resource["name"]),
                    )

                country_dataset = datasets[1]
                country_dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                country_name = country_dataset["title"].split(" - ")[0]
                country_dataset["notes"] = (
                    country_dataset["notes"].rstrip(".") + f" for {country_name}."
                )
                assert country_dataset == EXPECTED_COUNTRY_DATASET
                country_resources = country_dataset.get_resources()
                assert country_resources == EXPECTED_COUNTRY_RESOURCES
                for resource in country_resources:
                    assert_files_same(
                        join(tempdir, resource["name"]),
                        join(input_dir, resource["name"]),
                    )
