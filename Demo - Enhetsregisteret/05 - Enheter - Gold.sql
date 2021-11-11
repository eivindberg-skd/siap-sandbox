-- Databricks notebook source
CREATE LIVE TABLE gold_enhet as
select a.org_nr, a.antall_ansatte, a.naeringskode, b.aar, b.omsetning from live.silver_brreg_enhet a
inner join live.bronze_enheter_aar_omsetning b on a.org_nr = b.organisasjonsnummer
inner join live.silver_brreg_underenheter c on a.org_nr = c.organisasjonsnummer;


