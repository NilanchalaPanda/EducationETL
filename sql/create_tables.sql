-- Year Dimension
CREATE TABLE dim_year
(
    year_id SERIAL PRIMARY KEY,
    year INT NOT NULL
);

-- Location Dimension
CREATE TABLE dim_location
(
    location_id SERIAL PRIMARY KEY,
    college_location TEXT NOT NULL
);

-- Institute Dimension
CREATE TABLE dim_institute
(
    institute_id SERIAL PRIMARY KEY,
    college_name TEXT NOT NULL
);


-- FACT TABLE
CREATE TABLE fact_student_distribution
(
    fact_id SERIAL PRIMARY KEY,
    student_id UUID NOT NULL,
    age INT,
    department TEXT,
    midterm_score INT,
    final_score INT,
    attendance FLOAT,
    average_score DOUBLE PRECISION,
    grade TEXT,
    institute_id INT,
    location_id INT,
    college_name TEXT,
    college_location TEXT,
    FOREIGN KEY
    (institute_id) REFERENCES dim_institute
    (institute_id),
    FOREIGN KEY
    (location_id) REFERENCES dim_location
    (location_id)
);