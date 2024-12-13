-- Créer l'extension dblink si elle n'existe pas
CREATE EXTENSION IF NOT EXISTS dblink;

-- Insertion des données dans la table dimratecodes
INSERT INTO dimratecodes (ratecodeid, ratedescription)
SELECT DISTINCT "RatecodeID", 
       CASE 
           WHEN "RatecodeID" = 1 THEN 'Standard rate'
           WHEN "RatecodeID" = 2 THEN 'JFK'
           WHEN "RatecodeID" = 3 THEN 'Newark'
           WHEN "RatecodeID" = 4 THEN 'Nassau or Westchester'
           WHEN "RatecodeID" = 5 THEN 'Negotiated fare'
           WHEN "RatecodeID" = 6 THEN 'Group ride'
           ELSE 'Unknown'
       END 
FROM dblink(
    'host=db port=5432 dbname=yellow_taxi user=admin password=admin', 
    'SELECT DISTINCT "RatecodeID" FROM "yellow_taxi" WHERE "RatecodeID" IS NOT NULL'
) AS source_data("RatecodeID" INTEGER);

-- Insertion des données dans la table dimpaymenttypes
INSERT INTO dimpaymenttypes (paymenttypeid, paymentdescription)
SELECT DISTINCT "payment_type", 
       CASE 
           WHEN "payment_type" = 1 THEN 'Credit card'
           WHEN "payment_type" = 2 THEN 'Cash'
           WHEN "payment_type" = 3 THEN 'No charge'
           WHEN "payment_type" = 4 THEN 'Dispute'
           WHEN "payment_type" = 5 THEN 'Unknown'
           WHEN "payment_type" = 6 THEN 'Voided trip'
           ELSE 'Other'
       END 
FROM dblink(
    'host=db port=5432 dbname=yellow_taxi user=admin password=admin', 
    'SELECT DISTINCT "payment_type" FROM "yellow_taxi" WHERE "payment_type" IS NOT NULL'
) AS source_data("payment_type" INTEGER);

-- Insertion des données dans la table dimlocations (PULocationID)
INSERT INTO dimlocations (locationid, zonename, borough)
SELECT DISTINCT "PULocationID", 
       'Unknown' AS zonename,  -- Valeur par défaut pour zonename
       'Unknown' AS borough    -- Valeur par défaut pour borough
FROM dblink(
    'host=db port=5432 dbname=yellow_taxi user=admin password=admin', 
    'SELECT DISTINCT "PULocationID" FROM "yellow_taxi" WHERE "PULocationID" IS NOT NULL'
) AS source_data("PULocationID" INTEGER)
WHERE NOT EXISTS (
    SELECT 1 FROM dimlocations WHERE locationid = source_data."PULocationID"
);

-- Insertion des données dans la table facttrips
INSERT INTO facttrips (
    vendorid, pulocationid, dolocationid, ratecodeid, payment_type, fare_amount, 
    extra, mta_tax, improvement_surcharge, tip_amount, tolls_amount, total_amount, 
    congestion_surcharge, airport_fee, trip_distance, passenger_count, 
    tpep_pickup_datetime, tpep_dropoff_datetime
)
SELECT 
    "VendorID", 
    "PULocationID", 
    "DOLocationID", 
    "RatecodeID", 
    "payment_type", 
    "fare_amount", 
    "extra", 
    "mta_tax", 
    "improvement_surcharge", 
    "tip_amount", 
    "tolls_amount", 
    "total_amount", 
    "congestion_surcharge", 
    "airport_fee", 
    "trip_distance", 
    "passenger_count", 
    "tpep_pickup_datetime", 
    "tpep_dropoff_datetime"
FROM dblink(
    'host=db port=5432 dbname=yellow_taxi user=admin password=admin', 
    'SELECT "VendorID", "PULocationID", "DOLocationID", "RatecodeID", "payment_type", 
            "fare_amount", "extra", "mta_tax", "improvement_surcharge", "tip_amount", 
            "tolls_amount", "total_amount", "congestion_surcharge", "airport_fee", 
            "trip_distance", "passenger_count", "tpep_pickup_datetime", "tpep_dropoff_datetime"
     FROM "yellow_taxi" 
     WHERE "VendorID" IS NOT NULL AND "PULocationID" IS NOT NULL AND "DOLocationID" IS NOT NULL'
) AS source_data(
    "VendorID" INTEGER, "PULocationID" INTEGER, "DOLocationID" INTEGER, "RatecodeID" INTEGER, 
    "payment_type" INTEGER, "fare_amount" FLOAT, "extra" FLOAT, "mta_tax" FLOAT, 
    "improvement_surcharge" FLOAT, "tip_amount" FLOAT, "tolls_amount" FLOAT, 
    "total_amount" FLOAT, "congestion_surcharge" FLOAT, "airport_fee" FLOAT, 
    "trip_distance" FLOAT, "passenger_count" INTEGER, 
    "tpep_pickup_datetime" TIMESTAMP, "tpep_dropoff_datetime" TIMESTAMP
);

-- Insertion des données dans la table dimvendors
INSERT INTO dimvendors (vendorid, vendorname)
SELECT DISTINCT "VendorID", 
       CASE 
           WHEN "VendorID" = 1 THEN 'Creative Mobile Technologies, LLC' 
           WHEN "VendorID" = 2 THEN 'VeriFone Inc.' 
           ELSE 'Unknown' 
       END 
FROM dblink(
    'host=172.18.0.2 port=5432 dbname=yellow_taxi user=admin password=admin', 
    'SELECT DISTINCT "VendorID" FROM "yellow_taxi"'
) AS source_data("VendorID" INTEGER);
