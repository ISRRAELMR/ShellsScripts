#
# Objetivo : Consolidar CDRs de GPRS a la entrada del mediador
#
# Creado por:  Israel Melendez Romero
################################################################

FECHA=`date +%Y%m%d%H%M`

PATH_CHARGIN=/mediacion_GPRS/gemo/files_tmp/
PATH_RAIZ=/mediacion_GPRS/Consolida/entrada
PATH_TMP=/mediacion_GPRS/Consolida/tmp
PATH_SALIDA=/mediacion_GPRS/Consolida/salida
PATH_LOG=/mediacion_GPRS/Consolida/log
PATH_RESPALDO=/mediacion_GPRS/Consolida/bkup_in
PATH_DESCARTADOS=/mediacion_GPRS/Consolida/descartados
#TIP_ARCHIVO=processed
PATH_SHELL=/mediacion_GPRS/Consolida/scripts/shell
PATH_MEDIACION=/mediacion_GPRS/entrada

TIP_ARCHIVO=$1  # valores del tipo processed y roamer
B_EJECUCION=${PATH_LOG}/WORKING_${TIP_ARCHIVO}.txt
BITACORA_P=${PATH_LOG}/LOG_GENERAL_${TIP_ARCHIVO}_${FECHA}.log


#PATH_RAIZ=$1
#PATH_TMP=$2
#PATH_SALIDA=$3
#PATH_LOG=$4
#PATH_RESPALDO=$5
#TIP_ARCHIVO=$6  # valores del tipo processed y roamer
#PATH_SHELL=$7
#PATH_MEDIACION=$8

CHARGIN_1=CGMX3
CHARGIN_2=CGMX4
CHARGIN_3=CGMX5
CHARGIN_4=CGMX6
CHARGIN_5=CGMX7
CHARGIN_6=CGMX8
CHARGIN_7=CGMX9
CHARGIN_8=CGMY1
CHARGIN_9=CGMY2
CHARGIN_10=CGMX2

# ===============  DEPURA LOGS 

cd ${PATH_LOG}
TOT_LOGS=`ls LOG_GENERAL_${TIP_ARCHIVO}_*|wc -l`
if [ ${TOT_LOGS} -gt 10 ]
then
  BORRAR=`echo ${TOT_LOGS} 10|awk '{n=$1-$2} {printf "%d",n}'`
  rm `ls -alt LOG_GENERAL_${TIP_ARCHIVO}_*|tail -${BORRAR}|awk '{print $9}'`
fi

# ===============  FIN DEPURA LOGS



echo "......................Inicia ejecucion de SHELL ConsolidaGPRS de archivos ${TIP_ARCHIVO}: ${FECHA}" >> ${BITACORA_P}

# ==========================================  Valida si no esta en ejecución el shell =============
ls ${B_EJECUCION}
if [ $? -eq 0 ]
then
   echo "CONSOLIDACION PREVIA CONTINUA EN EJECUCION, SE ABORTA PROCESAMIENTO" >> ${BITACORA_P}
   exit 0
fi
# ==========================================  Fin validacion de ejecucion =====================================


# Se activa bandera de ejecucion
touch ${B_EJECUCION}


#mueve archivos que se hallan quedado en la carpeta temporal a la ruta descartados
mv ${PATH_TMP}/${TIP_ARCHIVO}* ${PATH_DESCARTADOS}


# ======================================= valida cantidad de archivos a procesar ==================


TOT_ARCHIVOS=`ls ${PATH_RAIZ}/${TIP_ARCHIVO}*|wc -l|awk '{print $1}'`
if [ ${TIP_ARCHIVO} = "roamer" ]
then
   MIN_ARCHIVOS=75
else
   MIN_ARCHIVOS=2000
fi

if [ ${TOT_ARCHIVOS} -lt ${MIN_ARCHIVOS} ]
then
  echo "Cantidad de archivos a procesar insuficientes, SE ABORTA PROCESAMIENTO" >> ${BITACORA_P}
  rm ${B_EJECUCION}
  exit 0
fi


# =======================================  fin de validacion cantidad de archivos =================


# PROCESAMIENTO PARALELO DE ARCHIVOS GPRS

${PATH_SHELL}/AgrupaGPRS.sh ${PATH_RAIZ} ${PATH_TMP} ${PATH_SALIDA} ${PATH_LOG} ${PATH_RESPALDO} ${TIP_ARCHIVO} ${PATH_MEDIACION} ${CHARGIN_1} &
${PATH_SHELL}/AgrupaGPRS.sh ${PATH_RAIZ} ${PATH_TMP} ${PATH_SALIDA} ${PATH_LOG} ${PATH_RESPALDO} ${TIP_ARCHIVO} ${PATH_MEDIACION} ${CHARGIN_2} &
${PATH_SHELL}/AgrupaGPRS.sh ${PATH_RAIZ} ${PATH_TMP} ${PATH_SALIDA} ${PATH_LOG} ${PATH_RESPALDO} ${TIP_ARCHIVO} ${PATH_MEDIACION} ${CHARGIN_3} &
${PATH_SHELL}/AgrupaGPRS.sh ${PATH_RAIZ} ${PATH_TMP} ${PATH_SALIDA} ${PATH_LOG} ${PATH_RESPALDO} ${TIP_ARCHIVO} ${PATH_MEDIACION} ${CHARGIN_4} &
${PATH_SHELL}/AgrupaGPRS.sh ${PATH_RAIZ} ${PATH_TMP} ${PATH_SALIDA} ${PATH_LOG} ${PATH_RESPALDO} ${TIP_ARCHIVO} ${PATH_MEDIACION} ${CHARGIN_5} &
${PATH_SHELL}/AgrupaGPRS.sh ${PATH_RAIZ} ${PATH_TMP} ${PATH_SALIDA} ${PATH_LOG} ${PATH_RESPALDO} ${TIP_ARCHIVO} ${PATH_MEDIACION} ${CHARGIN_6} &
${PATH_SHELL}/AgrupaGPRS.sh ${PATH_RAIZ} ${PATH_TMP} ${PATH_SALIDA} ${PATH_LOG} ${PATH_RESPALDO} ${TIP_ARCHIVO} ${PATH_MEDIACION} ${CHARGIN_7} &
${PATH_SHELL}/AgrupaGPRS.sh ${PATH_RAIZ} ${PATH_TMP} ${PATH_SALIDA} ${PATH_LOG} ${PATH_RESPALDO} ${TIP_ARCHIVO} ${PATH_MEDIACION} ${CHARGIN_8} &
${PATH_SHELL}/AgrupaGPRS.sh ${PATH_RAIZ} ${PATH_TMP} ${PATH_SALIDA} ${PATH_LOG} ${PATH_RESPALDO} ${TIP_ARCHIVO} ${PATH_MEDIACION} ${CHARGIN_9} &
${PATH_SHELL}/AgrupaGPRS.sh ${PATH_RAIZ} ${PATH_TMP} ${PATH_SALIDA} ${PATH_LOG} ${PATH_RESPALDO} ${TIP_ARCHIVO} ${PATH_MEDIACION} ${CHARGIN_10} &


wait


# Se borra bandera de ejecucion
rm ${B_EJECUCION}

echo "FINALIZA EJECUCION DE SHELL ConsolidaGPRS: `date +%Y%m%d%H%M`................................" >> ${BITACORA_P}

exit 0

