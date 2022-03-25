from workfinder import default_s3_api

s3_resource = default_s3_api()
client = s3_resource.get_s3_connection().s3
paginator = client.meta.client.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket="public-eo-data", Prefix="stac_catalogs/cs_stac/")
for page in pages:
    for o in page['Contents']:
        print(o['Key'])
        client.meta.client.delete_object(Bucket='public-eo-data', Key=o['Key'])
