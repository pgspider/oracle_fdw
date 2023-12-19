export ORACLE_HOME=/opt/oracle/product/21c/dbhomeXE
export PATH=$ORACLE_HOME/bin:$PATH
export ORAENV_ASK=NO
export ORACLE_SHLIB=$ORACLE_HOME/lib
export ORACLE_SID=XE
export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH

sed -i 's/REGRESS =.*/REGRESS = oracle_fdw oracle_gis oracle_import oracle_join oracle_extra oracle_fdw_post selectfunc /' Makefile

make clean
make $1
make check $1 | tee make_check.out
