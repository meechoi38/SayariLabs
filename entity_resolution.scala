// import two data sets for entity resolution and check schema

val jsonSchema_ofac = spark
  .read
 // .option("multiLine", true) // <-- the trick
  .json("/FileStore/tables/ofac.jsonl")
  .schema

val df_ofac = spark.read.schema(jsonSchema_ofac)
       .json("/FileStore/tables/ofac.jsonl")

df_ofac.printSchema()
df_ofac.show()


val jsonSchema_gbr = spark
  .read
 // .option("multiLine", true) // <-- the trick
  .json("/FileStore/tables/gbr.jsonl")
  .schema


val df_gbr = spark.read.schema(jsonSchema_ofac)
       .json("/FileStore/tables/gbr.jsonl")

//df_gbr.printSchema()
display(df_gbr)

val distinctDfOfacType = df_ofac.select("type").distinct()
display(distinctDfOfacType)

val distinctDfGbrType = df_gbr.select("type").distinct()
display(distinctDfGbrType)

import org.apache.spark.sql.functions._

//create udf functions to clean up columns
//country: "Uganda (reportedly in prison as of September 2016)" -> Uganda
//nationality: "Indian" -> India
//birthdate: 03 dec 1930 -> 03-12-1930 or 01/04/1980 -> 01-04-1980
//note: display(parseDfGbr.filter("name = 'Said BAHAJI'")) - bad entry for nationality- we should fix it later

def fix_nationality(s: String) : String = (s != null) match  {
  case false => null
  case true => s.substring(0, s.length - 1) //remove "n" at the end: Indian -> India. This is not a rigorous way to do it. We should improve
}

def fix_birthdate(s: String) : String = (s != null) match  {
  case false => null
  case true => s.replace(" ", "-").replace("/", "-")
  |.replace("Jan", "01").replace("Feb", "02").replace("Mar", "03").replace("Apr", "04")
  |.replace("May", "05").replace("Jun", "06").replace("Jul", "07").replace("Aug", "08")
  |.replace("Sep", "09").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12")
  |.replace("circa", "").replace("00-00", "")
}

def remove_paren(s: String) : String = {
  if (s != null) {(s contains "(") match {
                 case true => {val indexVal = s.indexOf("("); s.substring(0, indexVal)}
                 case false => {s}}} 
  else 
     return null}

val remove_parenUDF = udf(remove_paren(_))
val fix_nationalityUDF = udf(fix_nationality(_))
val fix_birthdateUDF = udf(fix_birthdate(_))

// preprocessing to make blockers: blockers are used to group datasets that share similar features
//ofac

var sliceDfOfac = df_ofac
display(sliceDfOfac)

var parseDfOfac = (sliceDfOfac
                  .withColumn("addresses", explode($"addresses"))
                  .withColumn("country", $"addresses".getItem("country"))
                  .withColumn("nationality_item", explode($"nationality"))
                  .withColumn("reported_dates_of_birth_item", explode($"reported_dates_of_birth"))
                   )

display(parseDfOfac)

//val distinctBlockerOfac = parseDfOfac.select("blocker").distinct()
//display(distinctBlockerOfac)

parseDfOfac.createOrReplaceTempView("tempOfac") //udf fuctions are used to add more cols

val parseDfOfac_tv = spark.sql("SELECT * FROM tempOfac")

var parseDfOfac_tv_1 = (parseDfOfac_tv
                      .withColumn("country_modified", remove_parenUDF(col("country")))
                      .withColumn("birthdate_modified", fix_birthdateUDF(col("reported_dates_of_birth_item")))
                      //.withColumn("nationality_modified", fix_nationalityUDF(col("nationality_item"))) only for gbr
                      .withColumn("blocker_type_country", concat(col("type"), lit("-"), col("country_modified")))
                      .withColumn("blocker_type_birth_nationality", concat(col("type"), lit("-"), col("birthdate_modified"), lit("-"), col("nationality_item")))
                       )

parseDfOfac = parseDfOfac_tv_1

//display(parseDfOfac)   

val DfOfac_final = parseDfOfac.select("id", "name", "blocker_type_country", "blocker_type_birth_nationality")
display(DfOfac_final)


//gbr
var sliceDfGbr = df_gbr
display(sliceDfGbr)

var parseDfGbr = (sliceDfGbr   
                  .withColumn("id",monotonicallyIncreasingId) //id are all null so I provide the ids
                  .withColumn("addresses", explode($"addresses"))
                  .withColumn("country", $"addresses".getItem("country"))
                  .withColumn("nationality_item", explode($"nationality"))
                  .withColumn("reported_dates_of_birth_item", explode($"reported_dates_of_birth"))
                  )

display(parseDfGbr)
parseDfGbr.createOrReplaceTempView("tempGbr")

val parseDfGbr_tv = spark.sql("SELECT * FROM tempGbr")

var parseDfGbr_tv_1 = (parseDfGbr_tv
                      .withColumn("country_modified", remove_parenUDF(col("country")))
                      .withColumn("birthdate_modified", fix_birthdateUDF(col("reported_dates_of_birth_item")))
                      .withColumn("nationality_modified", fix_nationalityUDF(col("nationality_item")))
                      .withColumn("blocker_type_country", concat(col("type"), lit("-"), col("country_modified")))
                      .withColumn("blocker_type_birth_nationality", concat(col("type"), lit("-"), col("birthdate_modified"), lit("-"), col("nationality_modified")))
                       )

parseDfGbr = parseDfGbr_tv_1

//display(parseDfGbr) 
val DfGbr_final =  (parseDfGbr
               .select("id", "name", "blocker_type_country", "blocker_type_birth_nationality")
               .withColumnRenamed("id", "id_gbr")
               .withColumnRenamed("name", "name_gbr")
               .withColumnRenamed("blocker_type_country", "blocker_type_country_gbr")
               .withColumnRenamed("blocker_type_birth_nationality", "blocker_type_birth_nationality_gbr")
                    )

//


//find the same names between two data sources
//if the blockers are the same, we consider them as strong candidates for match

val join_result_names_all = DfOfac_final.as("a").join(DfGbr_final.as("b"), 
                                       $"a.name" === $"b.name_gbr"
                                       )

display(join_result_names_all)

// good matches

display(join_result_names_all.filter("blocker_type_country = blocker_type_country_gbr")        
       )
display(join_result_names_all.filter("blocker_type_birth_nationality = blocker_type_birth_nationality_gbr")        
       )

// strong matches
display(join_result_names_all.filter("blocker_type_country = blocker_type_country_gbr") 
                             .filter("blocker_type_birth_nationality = blocker_type_birth_nationality_gbr")
       )
