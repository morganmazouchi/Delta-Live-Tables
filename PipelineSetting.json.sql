-- Databricks notebook source
{
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5
            }
        }
    ],
    "development": true,
    "continuous": false,
    "libraries": [
        {
            "notebook": {
                "path": "/Users/mojgan.mazouchi@databricks.com/ETL- Data Engineering/DLT_CDC/notebooks/1-CDC_DataGenerator"
            }
        },
        {
            "notebook": {
                "path": "/Users/mojgan.mazouchi@databricks.com/ETL- Data Engineering/DLT_CDC/notebooks/2-Retail_DLT_CDC_sql" 
            }
        }
    ],
    "name": "CDC_blog",
    "storage": "dbfs:/home/mydir/myDB/dlt_storage",
    "configuration": {
        "
        "pipelines.applyChangesPreviewEnabled": "true",
         source": "/tmp/demo/cdc_raw"
    },
    "target": "myDB"
}
