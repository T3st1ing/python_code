################################################################################
# Stage Name: SFO_CURR_GDG
# Stage Type: PxSequentialFile
# Stage Inputs:  
# Stage Outputs: extract_curr_file
################################################################################
log.info("PxSequentialFile Stage: SFO_CURR_GDG - Read/Write data to file.")
 
extract_curr_file_SFO_CURR_GDG_1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["<path-to-s3>"],
        "recurse": True,
    },
    transformation_ctx="extract_curr_file_SFO_CURR_GDG",
)
extract_curr_file_SFO_CURR_GDG = extract_curr_file_SFO_CURR_GDG_1.toDF()
################################################################################
# Stage Name: SFO_PREV_GDG
# Stage Type: PxSequentialFile
# Stage Inputs:  
# Stage Outputs: extract_prev_file
################################################################################
log.info("PxSequentialFile Stage: SFO_PREV_GDG - Read/Write data to file.")
 
extract_prev_file_SFO_PREV_GDG_1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["<path-to-s3>"],
        "recurse": True,
    },
    transformation_ctx="extract_prev_file_SFO_PREV_GDG",
)
extract_prev_file_SFO_PREV_GDG = extract_prev_file_SFO_PREV_GDG_1.toDF()
################################################################################
# Stage Name: COMP_INPUTS
# Stage Type: PxCompare
# Stage Inputs: extract_prev_file, extract_curr_file 
# Stage Outputs: comp_result
################################################################################
log.info("")
# PxCompare Stage: COMP_INPUTS  - Compares two inputs.
 
extract_prev_file_SFO_PREV_GDG.createOrReplaceTempView("extract_prev_file_SFO_PREV_GDG")
extract_curr_file_SFO_CURR_GDG.createOrReplaceTempView("extract_curr_file_SFO_CURR_GDG")
comp_result_COMP_INPUTS = spark.sql("""    
    select 
        bfr.record as first_result,
        aft.record as second_result,
        CASE 
            WHEN bfr.record = aft.record Then 0
            when bfr.record is null then -2
            when bfr.record > aft.record then 1
            when bfr.record < aft.record then -1
            when aft.record is null then 2 
        end as result         
    from 
        extract_prev_file_SFO_PREV_GDG bfr 
    FULL OUTER JOIN 
        extract_curr_file_SFO_CURR_GDG aft 
    on bfr.record = aft.record
""")
################################################################################
# Stage Name: AGG_COUNT_ROWS
# Stage Type: PxAggregator
# Stage Inputs: comp_result 
# Stage Outputs: filter_count
################################################################################
log.info("PxAggregator Stage: AGG_COUNT_ROWS - Runs an aggregation function over give columns.")
 
comp_result_COMP_INPUTS.createOrReplaceTempView("comp_result_COMP_INPUTS")
filter_count_AGG_COUNT_ROWS = spark.sql(""" select result ,count(*) as ROWS from comp_result_COMP_INPUTS group By result  """)
################################################################################
# Stage Name: FILTER_ZERO
# Stage Type: PxFilter
# Stage Inputs: filter_count 
# Stage Outputs: zero_1, zero_2, newdata
################################################################################
log.info("PxFilter Stage: FILTER_ZERO - filter records based on condition.")
# Write reject output: true
# Filter Conditions: 
# 	result = 0 and ROWS > 1
 
filter_count_AGG_COUNT_ROWS.createOrReplaceTempView("filter_count_AGG_COUNT_ROWS")
zero_1_FILTER_ZERO = spark.sql(""" select 
result
from filter_count_AGG_COUNT_ROWS 
where result = 0 and ROWS > 1  
""")
FILTER_ZERO_SELECT0.createOrReplaceTempView("FILTER_ZERO_SELECT0")
zero_2_FILTER_ZERO = spark.sql(""" select 
result
from filter_count_AGG_COUNT_ROWS 
where result = 0 and ROWS > 1  
""")
 
newdata_FILTER_ZERO = spark.sql(""" select 
result, ROWS
from filter_count_AGG_COUNT_ROWS 
where NOT(result = 0 and ROWS > 1)  
""")
################################################################################
# Stage Name: PLACEHOLDER_SUCCESS
# Stage Type: PxCopy
# Stage Inputs: newdata 
# Stage Outputs: 
################################################################################
log.info("PxCopyStage: PLACEHOLDER_SUCCESS - Copy data from input to given outputs.")
# No output record found to copy data.