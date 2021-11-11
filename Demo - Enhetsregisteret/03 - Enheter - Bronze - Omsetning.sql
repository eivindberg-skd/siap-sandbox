-- Databricks notebook source
CREATE INCREMENTAL LIVE TABLE bronze_enheter_aar_omsetning
AS SELECT * FROM cloud_files("/FileStore/shared_uploads/eivind.berg@skatteetaten.no/incremental", "csv")

