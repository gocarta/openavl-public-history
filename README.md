⚠️ WORK IN PROGRESS
# openavl-public-history
Work in Progress: Location History of Almost All CARTA Buses and Shuttles!

## background
We needed high resolution historical data on where our buses were.  It's very important to scheduling, troubleshooting issues, and performance management.

## frequency
The pipeline runs approximately every hour.

## columns
| column | example | description |
| :--- | :--- | :--- |
| **vehicle_id** | `"0135"` | The 4-letter unique internal identifier for the specific vehicle. |
| **timestamp** | `1775438284` | The number of seconds since the epoch in Unix time. |
| **latitude** | `35.0577` | The North-South geographic coordinate of the vehicle. |
| **longitude** | `-85.2691` | The East-West geographic coordinate of the vehicle. |
| **geometry** | `GEOMETRY('OGC:CRS84')` | The special geometry column only used in the data.parquet file |

## download links
- [metadata](https://gocarta.s3.us-east-2.amazonaws.com/public/data/openavl_public_history/v1/meta.json)
- [geoparquet](https://gocarta.s3.us-east-2.amazonaws.com/public/data/openavl_public_history/v1/data.parquet)

## preview links
- You can query the data with SQL using [duckdb](https://shell.duckdb.org/#queries=v0,CREATE-TABLE-dataset-AS-SELECT-*-FROM-'s3%3A%2F%2Fgocarta%2Fpublic%2Fdata%2Fopenavl_public_history%2Fv1%2Fdata.parquet'~,Describe-dataset~,SELECT-timestamp%2C-latitude%2C-longitude-FROM-dataset-WHERE-vehicle_id-%3D-'0135'-ORDER-BY-timestamp-DESC-LIMIT-10~).

## support
Post an issue [here](https://github.com/gocarta/openavl-public-history/issues) or email the package author at DanielDufour@gocarta.org.
