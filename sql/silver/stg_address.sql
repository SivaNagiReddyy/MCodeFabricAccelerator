-- silver.stg_address   <-  M query: Stg_Address
-- Source: HYDRA_BRONZE_LK.SalesLT.Address   |   load_type: Full
-- select + rename (StateProvince->State, CountryRegion->Country, ModifiedDate->LastModifiedDate),
-- type, trim City/State/Country, build FullAddress, de-duplicate on AddressID.
WITH src AS (
    SELECT
        CAST(AddressID AS BIGINT)                        AS AddressID,
        CAST(AddressLine1 AS STRING)                     AS AddressLine1,
        CAST(AddressLine2 AS STRING)                     AS AddressLine2,
        TRIM(CAST(City AS STRING))                       AS City,
        TRIM(CAST(StateProvince AS STRING))             AS State,
        TRIM(CAST(CountryRegion AS STRING))            AS Country,
        CAST(PostalCode AS STRING)                       AS PostalCode,
        TRY_CAST(ModifiedDate AS TIMESTAMP)            AS LastModifiedDate
    FROM HYDRA_BRONZE_LK.SalesLT.Address
),
dedup AS (
    -- M Table.Distinct keeps first-seen; here: latest LastModifiedDate wins (no dups expected on a PK)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY AddressID ORDER BY LastModifiedDate DESC NULLS LAST) AS _rn
    FROM src
)
SELECT
    AddressID, AddressLine1, AddressLine2, City, State, Country, PostalCode, LastModifiedDate,
    concat_ws(', ', AddressLine1, AddressLine2, City, State, PostalCode, Country) AS FullAddress
FROM dedup
WHERE _rn = 1
