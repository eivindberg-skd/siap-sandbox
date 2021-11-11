-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Formål med notebooken
-- MAGIC * Vise utpakking av nøstede strukturer fra json i sql
-- MAGIC * Vise expectations skrevet i SQL

-- COMMAND ----------


/* Kvalitetssjekker og navnenedringer*/
CREATE LIVE TABLE silver_brreg_enhet(
  CONSTRAINT valid_avvikling EXPECT  (under_avvikling = 'false') ON VIOLATION DROP ROW,
  CONSTRAINT valid_konkurs EXPECT  (konkurs = 'false'),
  CONSTRAINT valid_orgnr EXPECT  (org_nr IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT "Enhet med datakvalitetssjekker"
TBLPROPERTIES ("quality" = "bronze")
AS 
SELECT 
navn as org_navn,
organisasjonsnummer as org_nr,
organisasjonsform.kode as org_form_kode,
organisasjonsform.beskrivelse as org_form_beskrivelse,
registreringsdatoEnhetsregisteret as dato_registrert,
antallAnsatte as antall_ansatte,
forretningsadresse.adresse as adresse,
forretningsadresse.kommune as kommune,
forretningsadresse.postnummer as postnummer,
forretningsadresse.poststed as poststed,
frivilligMvaRegistrertBeskrivelser as frivillig_mva_registrert,
hjemmeside,
institusjonellSektorkode.beskrivelse as sektor_beskrivelse,
institusjonellSektorkode.kode as sektor_kode,
konkurs as konkurs,
maalform,
naeringskode1.beskrivelse as naeringskode_beskrivelse,
naeringskode1.kode as naeringskode,
underAvvikling as under_avvikling
FROM live.bronze_brreg_enheter;
