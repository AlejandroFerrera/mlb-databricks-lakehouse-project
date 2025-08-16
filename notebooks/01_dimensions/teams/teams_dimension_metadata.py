from dataclasses import dataclass

@dataclass
class LandingVolume:
    path: str
    partition_key: str
    file_name: str
    format: str

@dataclass
class TeamsDimensionMetadata:
    table_name: str
    landing_volume: LandingVolume

metadata = TeamsDimensionMetadata(
    table_name="teams",
    landing_volume=LandingVolume(
        path="/Volumes/mlb/landing/raw/teams",
        partition_key="ingestion_date",
        file_name="teams.json",
        format="json"
    )
)