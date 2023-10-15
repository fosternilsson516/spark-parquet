from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, monotonically_increasing_id, col


# Create a SparkSession
spark = SparkSession.builder.appName("JSON Processing").getOrCreate()

# Load JSON data into a DataFrame
df = spark.read.option("multiline", "true").json('json_data.json')



df_exploded = df.select(explode("users").alias("user_data"))

df_flat = df_exploded.select(
    col("user_data.id"),
    col("user_data.firstName"),
    col("user_data.lastName"),
    col("user_data.maidenName"),
    col("user_data.age"),
    col("user_data.gender"),
    col("user_data.email"),
    col("user_data.phone"),
    col("user_data.username"),
    col("user_data.password"),
    col("user_data.birthDate"),
    col("user_data.image"),
    col("user_data.bloodGroup"),
    col("user_data.height"),
    col("user_data.weight"),
    col("user_data.eyeColor"),
    col("user_data.hair.color"),
    col("user_data.hair.type"),
    col("user_data.domain"),
    col("user_data.ip"),
    col("user_data.macAddress"),
    col("user_data.university"),
    col("user_data.bank.cardExpire"),
    col("user_data.bank.cardNumber"),
    col("user_data.bank.cardType"),
    col("user_data.bank.currency"),
    col("user_data.bank.iban"),
    col("user_data.company.department"),
    col("user_data.company.name"),
    col("user_data.company.title"),
    col("user_data.ein"),
    col("user_data.ssn"),
    col("user_data.userAgent"),
    col("user_data.address.address").alias("user_address"),
    col("user_data.address.city").alias("user_city"),
    col("user_data.address.coordinates.lat").alias("user_lat"),
    col("user_data.address.coordinates.lng").alias("user_lng"),
    col("user_data.address.postalCode").alias("user_postalCode"),
    col("user_data.address.state").alias("user_state"),
    col("user_data.company.address.address").alias("company_address"),
    col("user_data.company.address.city").alias("company_city"),
    col("user_data.company.address.coordinates.lat").alias("company_lat"),
    col("user_data.company.address.coordinates.lng").alias("company_lng"),
    col("user_data.company.address.postalCode").alias("company_postalCode"),
    col("user_data.company.address.state").alias("company_state")
)


users = df_flat.select(
    "id", "firstName", "lastName", "maidenName", "age", "gender", "email", "phone",
    "username", "password", "birthDate", "image", "bloodGroup", "height", "weight", "eyeColor",
    "domain", "ip", "macAddress", "university", "ein", "ssn", "userAgent"
)

# Hair Table
hair = df_flat.select("color", "type").distinct()
hair = hair.withColumn("hair_id", monotonically_increasing_id())

# Address Table
address = df_flat.select(
    "id", "user_address", "user_city", "user_lat", "user_lng", "user_postalCode", "user_state"
).distinct()

# Company Table
company = df_flat.select(
    "id", "department", "name", "title", "company_address", "company_city", "company_lat", "company_lng", "company_postalCode", "company_state"
).distinct()

# Bank Table
bank = df_flat.select(
    "id", "cardExpire", "cardNumber", "cardType", "currency", "iban"
).distinct()

users.createOrReplaceTempView("users")
hair.createOrReplaceTempView("hair")
address.createOrReplaceTempView("address")
company.createOrReplaceTempView("company")
bank.createOrReplaceTempView("bank")


desc_age_blood_group_query = spark.sql("""
WITH BloodGroupAge AS (
    SELECT bloodGroup, age
    FROM users
)
SELECT bloodGroup, AVG(age) as average_age
FROM BloodGroupAge
GROUP BY bloodGroup
ORDER BY average_age DESC
""")

# To see the results
desc_age_blood_group_query.show()

comp_age_blood_query = spark.sql("""
WITH AgeGroups AS (
    SELECT 
        id,
        CASE
            WHEN age BETWEEN 0 AND 18 THEN '0-18'
            WHEN age BETWEEN 19 AND 35 THEN '19-35'
            WHEN age BETWEEN 36 AND 60 THEN '36-60'
            ELSE '61+'
        END AS age_category,
        bloodGroup
    FROM users
),
BloodGroupCounts AS (
    SELECT
        age_category,
        bloodGroup,
        COUNT(*) as user_count
    FROM AgeGroups
    GROUP BY age_category, bloodGroup
),
MaxCounts AS (
    SELECT
        age_category,
        MAX(user_count) as max_count
    FROM BloodGroupCounts
    GROUP BY age_category
)

SELECT 
    bgc.age_category,
    bgc.bloodGroup as most_common_bloodGroup,
    bgc.user_count
FROM BloodGroupCounts bgc
JOIN MaxCounts mc 
ON bgc.age_category = mc.age_category AND bgc.user_count = mc.max_count
ORDER BY bgc.age_category;
""")


comp_age_blood_query.show()

hair_state_query = spark.sql("""
WITH StateHairColorCTE AS (
    SELECT 
        a.user_state AS state,
        h.color AS hair_color,
        COUNT(u.id) AS user_count
    FROM users u
    JOIN address a ON u.id = a.id
    JOIN hair h ON u.id = h.hair_id
    GROUP BY a.user_state, h.color
)

SELECT 
    state, 
    hair_color, 
    user_count
FROM StateHairColorCTE
ORDER BY state, user_count DESC;

""")

hair_state_query.show()


output_path = "parquet_folder"

# Write to local Parquet files
"""
users.write.parquet(f"{output_path}/users")
hair.write.parquet(f"{output_path}/hair")
address.write.parquet(f"{output_path}/address")
company.write.parquet(f"{output_path}/company")
bank.write.parquet(f"{output_path}/bank")
"""
hair_state_query.write.parquet(f"{output_path}/hair_state_query")
comp_age_blood_query.write.parquet(f"{output_path}/comp_age_blood_query")
desc_age_blood_group_query.write.parquet(f"{output_path}/desc_age_blood_group_query")


spark.stop()