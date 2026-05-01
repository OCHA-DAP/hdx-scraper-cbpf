#!/usr/bin/python
"""Cbpf scraper"""

import logging
from datetime import UTC, datetime

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
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
            val = row.get("ProjectStartDate")
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
        return {r["name"]: r["id"] for r in existing.get_resources()}

    def generate_dataset(
        self, existing_resource_ids: dict | None = None
    ) -> Dataset | None:
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
            return None

        dataset = Dataset(
            {
                "name": self._configuration["dataset_name"],
                "title": self._configuration["dataset_title"],
            }
        )

        start_date, end_date = self._get_date_range(
            contributions_rows, project_summary_rows
        )
        dataset.set_time_period(start_date, end_date)
        dataset.add_tags(self._configuration["tags"])
        dataset.add_other_location("world")

        for filename, rows, description in [
            (
                "Contributions.csv",
                contributions_rows,
                self._configuration["contributions_description"],
            ),
            (
                "ProjectSummary.csv",
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
            dataset.generate_resource(
                folder=self._tempdir,
                filename=filename,
                rows=rows,
                resourcedata=resource_data,
                headers=list(rows[0].keys()),
            )

        return dataset
