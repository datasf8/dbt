
----------------------------------------------------------------------------------------
                        PolarSled Snowflake Python Executor v0.4
----------------------------------------------------------------------------------------

		 The aim of the tool is to execute DDL/DML on Snowflake using Python.

----------------------------------------------------------------------------------------
 Installation:
		Install Python (Python 3.6.5)
		Install Required Dependencies 
		(logging,sqlparse,pandas,requests,snowflake-python-connector)

----------------------------------------------------------------------------------------

	 Steps to use utility:
		 1: Extract zip file
		 2: Store files to be executed into input folder (Preferred format: .sql)
		 3: Store your password at any given location on your local desktop and 
		 	add it's complete location in password_location option in 
			configExecution.properties
		 4: Enter your credentials and login parameters in configConnection.properties
		 5: Before execution make sure configExecution.properties 
			fields are set to either to TRUE or FALSE (Refer Config Fields)
		 6: Add filenames to be executed in text file. eg: executionFileNames.txt
		 	If the filenames are in different path enable full_path_provided option 
		 7: If variable substitution is TRUE, Update Mapping.csv file with values
		 8: Open Command prompt and navigate to folder location 
		 9: Type: python snowflakeExecutor.py -f executionFileNames.txt -c connectionName
		10: Once the execution is complete find result/logs code in output folder

----------------------------------------------------------------------------------------

	 Notes: 
		 1: This tool will not work for Javascript/SQL based Procedures DDL.
		 (Feature will be added in future versions) 
		 2: Calling procedures will work.
		 3: DEBUG Logging available in Output folder in file LoggerDEBUG.log
		 4: To fill executionFileNames.txt you can navigate to input folder and open 
		    command prompt and type: dir /b and copy paste output in file.

	 Config Fields:
		 
		 sso_login = FALSE 
		 (Will make a standard connection mentioned in [snowflake_direct]
		  in configConnection.properties, if True will use [snowflake_sso])
		 
		 continue_on_failure = TRUE
		 (The code execution will continue execution even if a failure 
		  occured in any of the executed sql queries)
		  
		 variable_substitution = TRUE
		 (If true, values will be fetched from Mapping.csv, Format for 
		  variable substitution 
		  eg: 
		  example.sql: use database '&database_name';
		  values in Mapping.csv: database_name,testing
		  Output query which will execute: use database testing;)
		 
		 show_errors_on_console = True
		  (While running will show FileName, Query, Query Id, Error on console)
		  
		 append_master_csv_log = True
		  (Will append MASTER_LOG.csv file with output, If set to FALSE
		   will create a new csv file with filename being the current timestamp)
		
		 full_path_provided = TRUE
		 (Will let users add full path in input file list)
		
		 split_on_parameter=TRUE
		 (Will read each sql query assuming no additional ';' exists in query
		  If set to False: Will read entire file instead of splitting on ';')

		 default_connection = snowflake_sso
		 (Will be used when -c "Connection Paramter" not provided while executing
		 Script. Change the above name to any default SSO Connection. Make sure
		 sso_login flag is on/off according to default connection)

		 default_file_list_name= executionFileNames.txt
		 (Will use fileName set in this parameter when -f parameter not passed while
		 executing the script. Change the above name to any default input file name)
		 
		 password_location = C:\Users\10663793\Desktop\EntirePath\password.properties
		 Set password file path in password_location

	 Sample Password File:
	 --------------------------------
	        password.properties
	 --------------------------------
	 [password]=
	 password_sf=XXXXXXXXXX
	 password_sso=XXXXXXXXXX
	 password_sso_poc=XXXXXXXXXX
	 --------------------------------
		   
----------------------------------------------------------------------------------------
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *  
----------------------------------------------------------------------------------------

