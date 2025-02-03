
-- Create table for FactTrips
CREATE TABLE FactTrips (
    TripID SERIAL PRIMARY KEY,
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    Passenger_count INT,
    Trip_distance FLOAT,
    PULocationID INT,
    DOLocationID INT,
    RateCodeID INT,
    Store_and_fwd_flag CHAR(1),
    Payment_type INT,
    Fare_amount FLOAT,
    Extra FLOAT,
    MTA_tax FLOAT,
    Improvement_surcharge FLOAT,
    Tip_amount FLOAT,
    Tolls_amount FLOAT,
    Total_amount FLOAT,
    Congestion_Surcharge FLOAT,
    Airport_fee FLOAT
);

-- Create table for DimVendors
CREATE TABLE DimVendors (
    VendorID INT PRIMARY KEY,
    VendorName VARCHAR(255)
);

-- Create table for DimLocations
CREATE TABLE DimLocations (
    LocationID INT PRIMARY KEY,
    ZoneName VARCHAR(255),
    Borough VARCHAR(255)
);

-- Create table for DimRateCodes
CREATE TABLE DimRateCodes (
    RateCodeID INT PRIMARY KEY,
    RateDescription VARCHAR(255)
);

-- Create table for DimPaymentTypes
CREATE TABLE DimPaymentTypes (
    PaymentTypeID INT PRIMARY KEY,
    PaymentDescription VARCHAR(255)
);

-- Add foreign key constraints
ALTER TABLE FactTrips
ADD CONSTRAINT fk_vendor
FOREIGN KEY (VendorID) REFERENCES DimVendors(VendorID);

ALTER TABLE FactTrips
ADD CONSTRAINT fk_pulocation
FOREIGN KEY (PULocationID) REFERENCES DimLocations(LocationID);

ALTER TABLE FactTrips
ADD CONSTRAINT fk_dolocation
FOREIGN KEY (DOLocationID) REFERENCES DimLocations(LocationID);

ALTER TABLE FactTrips
ADD CONSTRAINT fk_ratecode
FOREIGN KEY (RateCodeID) REFERENCES DimRateCodes(RateCodeID);

ALTER TABLE FactTrips
ADD CONSTRAINT fk_paymenttype
FOREIGN KEY (Payment_type) REFERENCES DimPaymentTypes(PaymentTypeID);
