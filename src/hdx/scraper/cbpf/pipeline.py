#!/usr/bin/python
"""Cbpf scraper"""

import logging
import re
from datetime import UTC, datetime

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

    def _fetch_odata(self, url: str, filename: str) -> list:
        data = self._retriever.download_json(url, filename=filename)
        return data.get("value", [])

    def _get_date_range(
        self, contributions_rows: list, project_summary_rows: list
    ) -> tuple:
        start_dates = []
        end_dates = []

        for row in contributions_rows:
            val = row.get("PledgeDate")
            if val:
                dt = datetime.fromisoformat(val).replace(tzinfo=UTC)
                if dt.year >= 2000:
                    start_dates.append(dt)
                    end_dates.append(dt)

        for row in project_summary_rows:
            val = row.get("DateSubmitted")
            if val:
                dt = datetime.fromisoformat(val).replace(tzinfo=UTC)
                if dt.year >= 2000:
                    start_dates.append(dt)
                    end_dates.append(dt)

        start = min(start_dates) if start_dates else datetime.now(tz=UTC)
        end = max(end_dates) if end_dates else datetime.now(tz=UTC)
        return start, end

    def _get_resource_id_map(self) -> dict:
        existing = Dataset.read_from_hdx(self._configuration["dataset_name"])
        if not existing:
            return {}
        by_name = {r["name"]: r["id"] for r in existing.get_resources()}
        legacy_map = self._configuration.get("legacy_resource_name_map", {})
        result = {}
        for old_name, new_name in legacy_map.items():
            if old_name in by_name:
                result[new_name] = by_name[old_name]
            elif new_name in by_name:
                result[new_name] = by_name[new_name]
        return result

    def _get_preserved_resources(self) -> list:
        existing = Dataset.read_from_hdx(self._configuration["dataset_name"])
        if not existing:
            return []
        return [r for r in existing.get_resources() if r["format"].lower() != "csv"]

    def generate_datasets(
        self, existing_resource_ids: dict | None = None
    ) -> list[Dataset]:
        resource_id_map = (
            existing_resource_ids
            if existing_resource_ids is not None
            else self._get_resource_id_map()
        )

        contributions_rows = self._fetch_odata(
            self._configuration["contribution_url"], "Contributions.json"
        )
        project_summary_rows = self._fetch_odata(
            self._configuration["project_summary_url"], "ProjectSummary.json"
        )

        if not contributions_rows and not project_summary_rows:
            return []

        datasets = []

        # Global dataset
        global_dataset = Dataset(
            {
                "name": self._configuration["dataset_name"],
                "title": self._configuration["dataset_title"],
            }
        )
        start_date, end_date = self._get_date_range(
            contributions_rows, project_summary_rows
        )
        global_dataset.set_time_period(start_date, end_date)
        global_dataset.add_tags(self._configuration["tags"])
        global_dataset.add_other_location("world")

        for filename, rows, description in [
            (
                "global_cbpf_contributions.csv",
                contributions_rows,
                self._configuration["contributions_description"],
            ),
            (
                "global_cbpf_project_summary.csv",
                project_summary_rows,
                self._configuration["project_summary_description"],
            ),
        ]:
            if not rows:
                logger.warning(f"No data for {filename}, skipping")
                continue
            resource_data = {
                "name": filename,
                "description": description,
            }
            if filename in resource_id_map:
                resource_data["id"] = resource_id_map[filename]
            global_dataset.generate_resource(
                folder=self._tempdir,
                filename=filename,
                rows=rows,
                resourcedata=resource_data,
                headers=list(rows[0].keys()),
            )
        if existing_resource_ids is None:
            for resource in self._get_preserved_resources():
                global_dataset.add_update_resource(resource, ignore_datasetid=True)
        datasets.append(global_dataset)

        # Country datasets
        contribution_url = self._configuration["contribution_url"]
        project_summary_url = self._configuration["project_summary_url"]
        fund_abbrv_map = {
            r["PooledFundId"]: r["PooledFundCodeAbbrv"]
            for r in contributions_rows
            if r.get("PooledFundCodeAbbrv")
        }

        fund_ids = sorted(
            {r["PooledFundId"] for r in contributions_rows}
            | {r["PooledFundId"] for r in project_summary_rows}
        )

        for fund_id in fund_ids:
            fund_contributions = [
                r for r in contributions_rows if r["PooledFundId"] == fund_id
            ]
            fund_projects = [
                r for r in project_summary_rows if r["PooledFundId"] == fund_id
            ]

            fund_name = (
                fund_contributions[0]["PooledFundName"]
                if fund_contributions
                else fund_projects[0]["PooledFundName"]
            )

            display_name = re.sub(r"\s*\(.*?\)", "", fund_name).strip()
            iso3, _ = Country.get_iso3_country_code_fuzzy(display_name)
            if not iso3:
                logger.warning(f"Could not find ISO3 for {fund_name}, skipping")
                continue

            iso3_lower = iso3.lower()
            title = f"{display_name} - CBPF Allocations and Contributions"
            dataset_name = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")

            country_dataset = Dataset({"name": dataset_name, "title": title})
            start_date, end_date = self._get_date_range(
                fund_contributions, fund_projects
            )
            country_dataset.set_time_period(start_date, end_date)
            country_dataset.add_tags(self._configuration["tags"])
            country_dataset.add_country_location(iso3)

            abbrv = fund_abbrv_map.get(fund_id)
            abbrv_param = f"?poolfundAbbrv={abbrv}" if abbrv else ""
            for filename, rows, description in [
                (
                    f"{iso3_lower}_cbpf_contributions.csv",
                    fund_contributions,
                    f"This csv contains all the contributions for {display_name} in the pooled funds system. See live data from the CBPF API for {display_name} [here]({contribution_url}{abbrv_param}).",
                ),
                (
                    f"{iso3_lower}_cbpf_project_summary.csv",
                    fund_projects,
                    f"This csv contains the project summaries for all approved projects in {display_name}. See live data from the CBPF API for {display_name} [here]({project_summary_url}{abbrv_param}).",
                ),
            ]:
                if not rows:
                    logger.warning(f"No data for {filename}, skipping")
                    continue
                country_dataset.generate_resource(
                    folder=self._tempdir,
                    filename=filename,
                    rows=rows,
                    resourcedata={"name": filename, "description": description},
                    headers=list(rows[0].keys()),
                )

            datasets.append(country_dataset)

        return datasets
