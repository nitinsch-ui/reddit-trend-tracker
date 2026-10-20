# ADR-006: SeaweedFS as the local S3-compatible store

**Status:** accepted

## Context
The lakehouse should use the same `s3a://` code locally and on AWS. The
original plan was MinIO, but MinIO no longer publishes free, versioned
container images.

## Decision
Use SeaweedFS (`weed server -s3`, pinned to 3.97), configured from `.env`
with one access key. Job 1 creates the bucket at start-up with boto3.

## Consequences
- The same Hadoop S3A configuration works against AWS S3 by changing the endpoint and credentials.
- Small memory footprint (under 256 MB).
- Fallback: `LAKE_ROOT=file:///lake` puts the Delta tables on a Docker volume with no object store at all.
