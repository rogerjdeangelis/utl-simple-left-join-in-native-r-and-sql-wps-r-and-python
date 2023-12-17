# utl-simple-left-join-in-native-r-and-sql-wps-r-and-python
Simple left join in native r and sql wps r and python
    %let pgm=utl-simple-left-join-in-native-r-and-sql-wps-r-and-python;

    Simple left join in native r and sql wps r and python

      SOLUTIONS

         1 wps sql
         2 wps r sql
         3 wps python sql

    github
    https://tinyurl.com/3srn37m8
    https://github.com/rogerjdeangelis/utl-simple-left-join-in-native-r-and-sql-wps-r-and-python


    SOAPBOX ON

      Op commented: 'I will use the more readible sql version.'

      However keep in mid R and Pyhton can solve problems not suited for sql and the
      packages are indedspensible.

      This problem is made for SQL.

      I could not get any of the R native solutions to work, very Klingon?

      Somewhat obscure
      want<-transact[with(master[match(Y$ID, ID)],between(Y$DATE, START, END))][master, on=.(ID)] ID DATE Y START END x;

          '.(ID)]' and '))][master' are not obvious

      I don't see a problem with sql performance since R and python to a;lesser extent, fo not handle
      big data very well (big data is single table over 1tb).

    SOAPBOX OFF

    /*               _     _
     _ __  _ __ ___ | |__ | | ___ _ __ ___
    | `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
    | |_) | | | (_) | |_) | |  __/ | | | | |
    | .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
    |_|
    */

    /**************************************************************************************************************************/
    /*                                              |                                         |                               */
    /* ADD TRANSACTION COLUMNS TO MASTER DATASET    |                                         |                               */
    /*                                              |                                         |                               */
    /*                INPUT                         |          PROCESS                        |        OUTPUT                 */
    /*                =====                         |          ========                       |        ======                 */
    /*  MASTER                      TRANSACT        |                                         |         WANT                  */
    /*  =======================     =============== |                                         | ============================  */
    /*  ID    START    END    X     ID    DATE    Y |  add columns from transact to           | ID  START  END   X   DATE  Y  */
    /*                                              |  master where transaction date is       |                               */
    /*   1      1       5     A      3      1     c |  between start and end date of maxter   |  1    1     5    A     .      */
    /*   2      2       6     B      4      5     d |  and master id = transaction id         |  2    2     6    B     .      */
    /*   3      3       7     C      5      7     e |                                         |  3    3     7    C     .      */
    /*   4      4       8     D                     |  select                                 |  4    4     8    D     5   d  */
    /*   5      5       9     E                     |     l.*                                 |  5    5     9    E     7   e  */
    /*                                              |    ,r.date * asdded columns             |                               */
    /*                                              |    ,r.y                                 | Note after match on id(3)     */
    /*                                              |  from                                   | the transact date(1) is not   */
    /*                                              |     master as l left join transact as r | between master 3 and 7        */
    /*                                              |  on                                     |                               */
    /*                                              |         l.id = r.id                     |                               */
    /*                                              |     and r.date between l.start and l.end|                               */
    /*                                              |                                         |                               */
    /*                                              |                                         |                               */
    /*                                              |                                         |                               */
    /**************************************************************************************************************************/


    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    options validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.master;
     input
      id start end x $1.;
    cards4;
    1 1 5 A
    2 2 6 B
    3 3 7 C
    4 4 8 D
    5 5 9 E
    ;;;;
    run;quit;

    data sd1.transact;
     input id date y $1.;
    cards4;
    3 1 c
    4 5 d
    5 7 e
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  MASTER                      TRANSACT                                                                                  */
    /*  =======================     ===============                                                                           */
    /*  ID    START    END    X     ID    DATE    Y                                                                           */
    /*                                                                                                                        */
    /*   1      1       5     A      3      1     c                                                                           */
    /*   2      2       6     B      4      5     d                                                                           */
    /*   3      3       7     C      5      7     e                                                                           */
    /*   4      4       8     D                                                                                               */
    /*   5      5       9     E                                                                                               */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                                  _
    / | __      ___ __  ___   ___  __ _| |
    | | \ \ /\ / / `_ \/ __| / __|/ _` | |
    | |  \ V  V /| |_) \__ \ \__ \ (_| | |
    |_|   \_/\_/ | .__/|___/ |___/\__, |_|
                 |_|                 |_|
    */

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    options validvarname=any;
    proc sql;
      create
         table sd1.want as
      select
         l.*
        ,r.date
        ,r.y
      from
         sd1.mast er as l left join sd1.transact as r
      on
             l.id = r.id
         and r.date between l.start and l.end
    ;quit;
    ');

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* ID    START    END    X    DATE    Y                                                                                   */
    /*                                                                                                                        */
    /*  1      1       5     A      .                                                                                         */
    /*  2      2       6     B      .                                                                                         */
    /*  3      3       7     C      .                                                                                         */
    /*  4      4       8     D      5     d                                                                                   */
    /*  5      5       9     E      7     e                                                                                   */
    /*                                                                                                                        */
    /**************************************************************************************************************************/


    /*___                                          _
    |___ \  __      ___ __  ___   _ __   ___  __ _| |
      __) | \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
     / __/   \ V  V /| |_) \__ \ | |    \__ \ (_| | |
    |_____|   \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                     |_|                        |_|
    */

    proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.master   r=master;
    export data=sd1.transact r=transact;
    submit;
    library(sqldf);
    want <- sqldf("
      select
         l.*
        ,r.date
        ,r.y
      from
         master as l left join transact as r
      on
             l.id = r.id
         and r.date between l .start and l.end
      ");
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    ');

    proc print data=sd1.want width=min;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  The WPS R System                                                                                                      */
    /*                                                                                                                        */
    /*    ID START END X DATE    Y                                                                                            */
    /*  1  1     1   5 A   NA <NA>                                                                                            */
    /*  2  2     2   6 B   NA <NA>                                                                                            */
    /*  3  3     3   7 C   NA <NA>                                                                                            */
    /*  4  4     4   8 D    5    d                                                                                            */
    /*  5  5     5   9 E    7    e                                                                                            */
    /*                                                                                                                        */
    /*  The WPS System                                                                                                        */
    /*                                                                                                                        */
    /* ID    START    END    X    DATE    Y                                                                                   */
    /*                                                                                                                        */
    /*  1      1       5     A      .                                                                                         */
    /*  2      2       6     B      .                                                                                         */
    /*  3      3       7     C      .                                                                                         */
    /*  4      4       8     D      5     d                                                                                   */
    /*  5      5       9     E      7     e                                                                                   */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*
     ____                                     _   _                             _
    |___ /  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
      |_ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
     ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
    |____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                     |_|         |_|    |___/                                |_|
    */

    proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

    %utl_submit_wps64x("
    options validvarname=any lrecl=32756;
    libname sd1 'd:/sd1';
    proc python;
    export data=sd1.master   python=master;
    export data=sd1.transact python=transact;
    submit;
    import pyreadstat;
    from os import path;
    import pandas as pd;
    import numpy as np;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    want = pdsql('''
      select
         l.*
        ,r.date
        ,r.y
      from
         master as l left join transact as r
      on
             l.id = r.id
         and r.date between l .start and l.end
    ''');
    print(want);
    pyreadstat.write_xport(want, 'd:/xpt/want.xpt',table_name='want',file_format_version=5);
    endsubmit;
    run;quit;
    ");

    libname xpt xport "d:/xpt/want.xpt";
    proc contents data=xpt._all_;
    run;quit;
    data want;
      set xpt.want;
    run;quit;
    proc print;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The PYTHON Procedure                                                                                                   */
    /*                                                                                                                        */
    /*     ID  START  END  X  DATE     Y                                                                                      */
    /* 0  1.0    1.0  5.0  A   NaN  None                                                                                      */
    /* 1  2.0    2.0  6.0  B   NaN  None                                                                                      */
    /* 2  3.0    3.0  7.0  C   NaN  None                                                                                      */
    /* 3  4.0    4.0  8.0  D   5.0     d                                                                                      */
    /* 4  5.0    5.0  9.0  E   7.0     e                                                                                      */
    /*                                                                                                                        */
    /* The WPS SYSTEM                                                                                                         */
    /*                                                                                                                        */
    /* ID    START    END    X    DATE    Y                                                                                   */
    /*                                                                                                                        */
    /*  1      1       5     A      .                                                                                         */
    /*  2      2       6     B      .                                                                                         */
    /*  3      3       7     C      .                                                                                         */
    /*  4      4       8     D      5     d                                                                                   */
    /*  5      5       9     E      7     e                                                                                   */
    /*                                                                                                                        */
    /**************************************************************************************************************************/


    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
