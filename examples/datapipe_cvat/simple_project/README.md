# CVAT Annotation Pipeline With Datapipe

## Overview

This example demonstrates a small Datapipe pipeline that discovers local images,
uploads them to S3-compatible storage, creates CVAT tasks, and synchronizes CVAT
annotations back into Datapipe tables.

## Description

The pipeline reads `input/{image_id}.jpeg`, writes the images to
`CVAT_EXAMPLE_S3_BUCKET`, batches them into CVAT tasks, and uses the bundled
sample annotations as optional preannotations.

## Prerequisites

- Installed monorepo packages: `datapipe-core` and `datapipe-cvat`.
- A CVAT project with S3/cloud storage configured.
- Credentials for the S3-compatible target used by `fsspec`.

## Setup

Download a tiny local image set:

```bash
python download_images.py
```

Configure runtime settings:

```bash
export CVAT_URL=http://localhost:8080
export CVAT_USERNAME=admin
export CVAT_PASSWORD=admin
export CVAT_PROJECT_ID=1
export CVAT_ORGANIZATION=
export CVAT_EXAMPLE_S3_BUCKET=s3://datapipe-cvat-example
export CVAT_EXAMPLE_CLOUD_STORAGE_BUCKET=s3://datapipe-cvat-example
```

## Usage

```bash
datapipe db create-all
datapipe step run
```
