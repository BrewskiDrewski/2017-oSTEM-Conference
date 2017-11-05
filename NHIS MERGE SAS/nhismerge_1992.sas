
 DM 'CLE LOG; CLE OUT';
 ************************************************************************
 SEPTEMBER 16, 2013
  Version 1
  
  
  THIS IS AN EXAMPLE OF A SAS PROGRAM THAT LINKS 1992 NHIS DATA DOWNLOADED
  FROM THE NCHS WEBSITE AND IHIS DATA IN SAS FILE FORMAT.

  NOTES:
    CERTAIN VARIABLES IN FOR LINKING, INCLUDING PROCYEAR QUARTER PSUNUMR WEEKCEN          
  	SEGNUM HHNUM PNUM, IN NHIS DATA ARE NEEDED AND NOT TO BE CHANGED BEFORE THE MERGE 

  	THIS SAS FILE WILL WORK FOR LINKING PERSON LEVEL FILE  
  	
For this program to work, users must replace the contents and brackets <  > 
 in each of the lines for:
 
 	- 3 LIBNAME statements in section 1
 	- 4 macro variables in subsection 1.1 - 1.4
	- 1 list of variable names in section 2 	

  	
      STEP 1: CHANGE DATA PATH AND FILE NAMES IN SECTION 1 AS NEEDED.
               SECTION 1.1: SPECIFY THE NAME OF IHIS FORMAT LIBRARY
               SECTION 1.2: SPECIFY THE NAME OF THE NHIS DATA (Merging file)
               SECTION 1.3: SPECIFY THE NAME OF THE IHIS DATA (Master file)
               SECTION 1.4: SPECIFY NAME OF FINAL MERGED DATA
	
      STEP 2: SPECIFY THE VARIABLES OF THE NHIS DATA TO BE KEPT IN SECTION 2 
    
      STEP 3: SAVE CHANGES TO SAS PROGRAM AND RUN
 *************************************************************************;


/***********************************************************************/
/*** SECTION 1: DATA PATH AND FILE NAMES; CHANGE FILE NAMES AS NEEDED***/
/***********************************************************************/

LIBNAME NHIS '<insert path to folder containing NHIS raw data here>';
LIBNAME IHIS '<insert path to folder containing IHIS data & format library here>';


/* Create macro variable NHISYEAR and set to year currently being processed */
%let nhisyear=1992;



/* SECTION 1.1: Create macro variable FMTLIB to identify format library. 
   Replace <insert IHIS format library name here> with the name of your IHIS format library. 
   Remove < and > and leave no spaces in your specification.  */

%let fmtlib=ihis.<insert IHIS format library name here>;


/* SECTION 1.2: Create macro variable NHISDSNAME and to identify NHIS data set to be used for merge. 
   Replace <insert NHIS raw data file name here> with the name of your NHIS file. 
   Remove < and > and leave no spaces in your specification. */

%let nhisdsname=nhis.<insert NHIS raw data file name here>;


/* SECTION 1.3: Create macro variable IHISDSNAME and to identify IHIS data set being processed (MASTER FILE). 
   Replace <insert IHIS data file name here> with the name of your IHIS file. 
   Remove < and > and leave no spaces in your specification.*/

%let ihisdsname=ihis.<insert IHIS raw data file name here>;


/* SECTION 1.4: Create macro variable DSMERGE to identify final merged data. 
   Replace <insert merge file name here> with the name of your final merged IHIS data file. 
   Remove < and > and leave no spaces in your specification.  */

%let dsmerge=ihis.<insert merge file name here>;

/***********************************************************************/
/*** SECTION 2: SPECIFY THE VARIABLES OF THE NHIS DATA TO BE KEPT IN****/
/***********************************************************************/

/* Replace <insert names of NHIS variables you would like to merge here> with the names  
   of the NHIS variables you want to merge to IHIS. 
   Separate the variables in your list with a space. Remove the < and >  */

%let indvars = <insert names of NHIS variables you would like to merge here>;






/***********************************************************************/
/***********************************************************************/
/***********************************************************************/



/*************************************************************/
/*** SECTION 3: PREPARING NHIS DATA TO MERGE WITH IHIS DATA***/
/*************************************************************/

/* SECTION 3.1: CHECK DUPLICATES FOR UNIQUE IDENTIFIER IN NHIS DATA */

PROC SORT DATA=&nhisdsname (keep=procyear quarter psunumr weekcen2 segnum hhnum pnum)  OUT=A1 NODUPKEY;
BY procyear quarter psunumr weekcen2 segnum hhnum pnum;
RUN;


/* SECTION 3.2: GENERATE LINKING KEY */

OPTIONS NOFMTERR;
DATA person&nhisyear;
  SET &nhisdsname;
	ATTRIB nhispid length=$16 LABEL='NHIS Person Unique Indentifier Key'
	       year length=4 LABEL='NHIS Year';
    year=&nhisyear;
  
nhispid=compress('19'||put(procyear,2.)||put(quarter,1.)||psunumr||put(weekcen,z2.)||segnum||hhnum||pnum);
RUN;

PROC SORT DATA=person&nhisyear;
BY year nhispid;
RUN;


/* SECTION 3.3: CHECK DUPLICATES LINKING IDENTIFIER IN NHIS DATA */

OPTIONS FULLSTIMER;
PROC SQL;
 TITLE 'CHECK DUPLICATES LINKING IDENTIFIER IN NHIS DATA' ;
 SELECT count(*) 'Total Records' AS total,
         count(DISTINCT cats(year,nhispid)) 'Unique Records' AS unique,
         calculated total- calculated unique 'Duplicates'
         INTO :totalrecs, :uniquerecs, :duprecs
     FROM person&nhisyear;
QUIT;

%PUT ****************************************************;
%PUT * NHIS duplicates check results:;
%PUT *   Total number of records in NHIS data set: &totalrecs;
%PUT *   Unique keys in NHIS data set: &uniquerecs;
%PUT *   Records that have a duplicate key: &duprecs;
%PUT ****************************************************;




/* SECTION 3.4: KEEP VARIABLES IN NHIS DATA */

OPTIONS NOFMTERR;
DATA person1&nhisyear;
  SET person&nhisyear;
  KEEP year nhispid &indvars;
RUN;


/*************************************************/
/*** SECTION 4: MERGE NHIS DATA WITH IHIS DATA ***/
/*************************************************/

/* SECTION 4.1: CHECK DUPLICATES OF LINKING KEY IN IHIS DATA */

PROC FORMAT cntlin=&fmtlib; 
RUN;
DATA ihis&nhisyear;
  SET &ihisdsname;
RUN; 

PROC SORT DATA=ihis&nhisyear;
  BY year nhispid;
RUN;

PROC SORT DATA=ihis&nhisyear(KEEP=year nhispid) OUT=A2 NODUPKEY;
  BY year nhispid;
RUN;


/* SECTION 4.2: MERGE NHIS DATA WITH IHIS DATA */

DATA &dsmerge ;
   MERGE ihis&nhisyear(in=in1) person1&nhisyear(in=in2);
   BY year nhispid;
   
ATTRIB _merge LENGTH=3 LABEL='DATA SET SOURCE FOR OBS';
   IF in1 and in2 then _merge=3;
   ELSE IF in1 and not in2 THEN _merge=1;
   ELSE IF not in1 and in2 THEN _merge=2;
RUN;    


/* SECTION 4.3: CHECK DUPLICATES OF LINKING KEY IN MERGE DATA  */

OPTIONS FULLSTIMER;
PROC SQL;
 TITLE 'DUPLICATES CHECK OF MERGED DATA SET';
 SELECT count(*) 'Total Records' AS total,
         count(DISTINCT cats(year,nhispid)) 'Unique Records' AS unique,
         calculated total- calculated unique 'Duplicates'
         INTO :totalrecs, :uniquerecs, :duprecs
     FROM &dsmerge;
QUIT;

%PUT ****************************************************;
%PUT * MERGE DATA duplicates check results:;
%PUT *   Total number of records in merged data set: &totalrecs;
%PUT *   Unique keys in merged data set: &uniquerecs;
%PUT *   Records that have a duplicate key: &duprecs;
%PUT ****************************************************;


/* SECTION 4.4: CHECK THE RESULTS OF THE MERGE IN THE SAS LOG  */

TITLE 'CHECK THE RESULTS OF THE MERGE DATA';
PROC FREQ DATA=&dsmerge;
    TABLES _merge/ OUT=__freqs ;
    BY year;

RUN;

%LET _merge1=0;
%LET _merge2=0;
%LET _merge3=0;
%LET _mergetotal=0;

DATA _null_;
  SET __freqs end=eof;
  IF _merge=1 THEN CALL symput('_MERGE1',count);
  ELSE IF _merge=2 THEN CALL symput('_MERGE2',count);
  ELSE IF _merge=3 THEN CALL symput('_MERGE3',count);

  _mergetotal+count;
  year=&nhisyear;
  
  IF eof THEN CALL symput('_MERGETOTAL',_mergetotal);
RUN;

PROC PRINT DATA=__freqs;
RUN;




