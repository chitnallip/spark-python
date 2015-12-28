# Import SQLContext and data types

from __future__ import print_function

import os
import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

# A function to convert the data into some delimited format
def toCSVLine(data):
  return '|'.join(str(d) for d in data)



def getDataFromFile (dataFile, sparkCtx) :
	return sparkCtx.textFile(dataFile).map( lambda l : l.split(","))


# This function loops through each fields and seperates the data into arrays
def doDataContexMap (dataCtxMap) : 
	dataContext = dataCtxMap.map( lambda P : (P[0],P[1],P[2],P[3],P[4],P[5],P[6],P[7],P[8],P[9],P[10],P[11],P[12],P[13],P[14],P[15],P[16],P[17],P[18],P[19],P[20],P[21],P[22],P[23],P[24],P[25],P[26],P[27],P[28],P[29],P[30],P[31],P[32],P[33],P[34],P[35],P[36],P[37],P[38],P[39],P[40],P[41],P[42],P[43],P[44],P[45],P[46],P[47],P[48],P[49],P[50],P[51],P[52],P[53],P[54],P[55],P[56],P[57],P[58],P[59],P[60],P[61],P[62],P[63],P[64],P[65],P[66],P[67],P[68],P[69],P[70],P[71],P[72],P[73],P[74],P[75],P[76],P[77],P[78],P[79],P[80],P[81],P[82],P[83]) )
	return dataContext

def getSchemaString() :
	return "INVENTORY_DATE TRN_ID TRN_NAME TRN_START_DATE TRN_END_DATE TRN_BUS_UNIT TRN_SBU_UNIT TRN_PORTFOLIO_UNIT TRN_BUS_ID TRN_SBU_ID TRN_PORTFOLIO_ID TRN_AMOUNT TRN_CCY TRN_RPT_CCY TRN_COUNTRY TRN_RWA CPTY_VAL CPTY_VAL_ISSUER ASS_CFLOW_IND EXC_CRR_RATIO_IND EXC_SLR_RATIO_IND NON_FIN_ISSUER_IND UNC_IND PRD_TYP_COD PRD_CAT_COD AST_TYPE ISSUER_ZERO_RWA_IND ISSUER_RWA_VALUE ISSUER_CREDIT_RATING BSE_NSE_COMPOSITE_IND INS_DEP_IND FOR_CCY_DEP_IND MAT_LESS_30_DAYS_IND COLLATERAL_IND COL_QAL_TYPE COL_CALLABLE_IND CPTY_RATING_TTIGGER_IND DERV_VALU_CHG_IND FAC_COM_IND FAC_DRAW_IND FAC_TYP_IND TB1 TB2 TB3 TB4 TB5 TB6 TB7 TB8 TB9 TB10 TB11 TB12 TB13 TB14 TB15 TB16 TB17 TB18 TB19 TB20 TB21 TB22 TB23 TB24 TB25 TB26 TB27 TB28 TB29 TB30 TB31 TB32 TB33 TB34 TB35 TB36 TB37 TB38 TB39 TB40 TB41 TB42 TB43 TB44 TB45 TB46 TB47"


def createSchemaString() :
	schemaString = getSchemaString()
	fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
	nrtSchema = StructType(fields)
	return nrtSchema
	

# Action begins here
if __name__ == "__main__":
	sc = SparkContext()
	sqlContext = SQLContext(sc)

	# Load a text file and convert each line to a array fields. Each line is an object
	# 1. the previous file (n-1)
	dataFromPreviousVersion = getDataFromFile("examples/src/main/prasad/files/data_small.DAT",sc)
	# 2. the current files (n)
	dataFromCurrentVersion = getDataFromFile("examples/src/main/prasad/files/data_big.DAT",sc)


	res1 = dataFromPreviousVersion.map( lambda t : (t[17], float(t[11])  )).reduceByKey( lambda x,y:x+y ).collect()
	res2 = dataFromCurrentVersion.map( lambda t : (t[17], float(t[11])  )).reduceByKey( lambda x,y:x+y ).collect()

	# Parallelize the RDD's
	result1 = sc.parallelize(res1);
	result2 = sc.parallelize(res2);

	out = result1.join(result2).map(toCSVLine).saveAsTextFile("examples/src/main/prasad/files/test.out")

	
	#out.map( toCSVLine(d)).saveAsTextFile("examples/src/main/prasad/files/test.out")

	# Set the data context; i.e. read the data as array of fields in each line
#	doDataContexMap(dataFromPreviousVersion)
#	doDataContexMap(dataFromCurrentVersion)

     # The schema is encoded in a string.
#	schema = createSchemaString()

	## Apply the schema to the RDD.
#	previousSchema = sqlContext.createDataFrame(dataFromPreviousVersion, schema)
#	currentSchema  = sqlContext.createDataFrame(dataFromCurrentVersion,  schema)

	## Register the DataFrame as a table.
#	previousSchema.registerTempTable("OLD")
#	currentSchema.registerTempTable("NEW")

	## SQL can be run over DataFrames that have been registered as a table.
	#results = sqlContext.sql("SELECT A.CPTY_VAL_ISSUER, COUNT(*) FROM OLD A, NEW B WHERE A.CPTY_VAL_ISSUER = B.CPTY_VAL_ISSUER GROUP BY A.CPTY_VAL_ISSUER")

#	result1 = sqlContext.sql("SELECT CPTY_VAL_ISSUER, COUNT(*) FROM OLD  GROUP BY CPTY_VAL_ISSUER")
#	result2 = sqlContext.sql("SELECT CPTY_VAL_ISSUER, COUNT(*) FROM NEW  GROUP BY CPTY_VAL_ISSUER")

#	res1out = sc.parallelize(result1)
#	res2out = sc.parallelize(result2)

#	res1out.join(res2out).map(toCSVLine).saveAsTextFile("examples/src/main/prasad/files/test.out")

	sc.stop()
