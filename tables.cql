-- BRONZE TABLE
CREATE TABLE IF NOT EXISTS diabetes_data (
    id UUID PRIMARY KEY,
    year int, gender text, age float, location text,
    race_africanamerican int, race_asian int, race_caucasian int,
    race_hispanic int, race_other int,
    hypertension int, heart_disease int,
    smoking_history text, bmi float, hba1c_level float,
    blood_glucose_level int, diabetes int
);

-- SILVER TABLE
CREATE TABLE IF NOT EXISTS diabetes_data_clean (
    id UUID PRIMARY KEY,
    year int, gender text, age float, location text,
    race_africanamerican int, race_asian int, race_caucasian int,
    race_hispanic int, race_other int,
    hypertension int, heart_disease int,
    smoking_history text, bmi float, hba1c_level float,
    blood_glucose_level int, diabetes int
);

-- GOLD TABLE 1: Aggregated age group stats
CREATE TABLE IF NOT EXISTS diabetes_age_group_stats (
    age_group text PRIMARY KEY,
    total int,
    diabetic int,
    percentage float
);

-- GOLD TABLE 2: ML model predictions
CREATE TABLE IF NOT EXISTS diabetes_predictions (
    patient_id uuid PRIMARY KEY,
    actual int,
    predicted int,
    age float,
    bmi float,
    blood_glucose_level int,
    hba1c_level float,
    hypertension int,
    heart_disease int
);

-- GOLD TABLE 3: Correlation matrix
CREATE TABLE IF NOT EXISTS diabetes_correlation_matrix (
    feature_1 text,
    feature_2 text,
    correlation float,
    PRIMARY KEY (feature_1, feature_2)
);
