# Workfinder

Find work for the ard processing.

Run with `python -m workfinder.workfinder LANDSAT8`


Environment variables

|Var name|Used for|
| ----------- | ----------- |
|APP_REGION| Region for which to generate work for.|
|APP_CRS| CRS Code of the region to generate work for.|
|APP_TEMP_DIR| Temp directory for files that are currently being processed.|
|COPERNICUS_USERNAME|Username for Copernicus API.|
|COPERNICUS_PASSWORD|Password for Copernicus API.|
|USGS_USERNAME|Username for USGS API.|
|USGS_PASSWORD|Password for USGS API.|
|USGS_API_ENDPOINT|API Endpoint for USGS API.|
|REDIS_HOST| The hostname of the REDIS server.|
|REDIS_PORT| The port of the REDIS server.|
|NATS_HOST | The hostname of the NATS server.|
|NATS_PORT | The port of the NATS server.|
|AWS_ACCESS_KEY_ID | AWS access key.|
|AWS_SECRET_ACCESS_KEY | AWS secret key.|
|AWS_DEFAULT_REGION | AWS region.|
|S3_ENDPOINT_URL | S3 endpoint url.|
|S3_BUCKET | S3 bucket name.|
|S3_IMAGERY_PATH| Path on the S3 Bucket in which to store the imagery.|
|S3_STAC_PATH|Path on the S3 Bucket in which to store the STAC metadata for processed imagery.|
|S1_REDIS_PROCESSED_CHANEL|Redis channel for S1 processed imagery download links and metadata.|
|S1_ARD_STAC_COLLECTION_PATH|Path to collection.json for S1_ARD data on the S3 Bucket. Parent directory is S3_IMAGERY_PATH.|
|S1_ARD_STAC_COLLECTION_PATH_MLWATER|Path to collection.json for S1_ARD data MLWATER on the S3 Bucket. Parent directory is S3_IMAGERY_PATH.|
|S2_REDIS_PROCESSED_CHANEL|Redis channel for S2 processed imagery download links and metadata.|
|S2_ARD_STAC_COLLECTION_PATH|Path to collection.json for S2_ARD data on the S3 Bucket. Parent directory is S3_IMAGERY_PATH.|
|S2_ARD_STAC_COLLECTION_PATH_MLWATER|Path to collection.json for S2_ARD data MLWATER on the S3 Bucket. Parent directory is S3_IMAGERY_PATH.|
|S2_ARD_STAC_COLLECTION_PATH_WOFS|Path to collection.json for S2_ARD data WOFS on the S3 Bucket. Parent directory is S3_IMAGERY_PATH.|

This package was created with Cookiecutter and the `audreyr/cookiecutter-pypackage` project template.
<br>
Free software: Apache Software License 2.0