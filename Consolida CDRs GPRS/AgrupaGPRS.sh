################################################################
#
# Objetivo : Consolidar CDRs de GPRS a la entrada del mediador
#
# Creado por:  Israel Melendez Romero
################################################################

FECHA=`date +%Y%m%d`

#PATH_RAIZ=/export/home/bgw/GPRSconsolida
#PATH_TMP=/export/home/bgw/GPRSconsolida/tmp
#PATH_SALIDA=/export/home/bgw/GPRSconsolida/salida
#PATH_LOG=/export/home/bgw/GPRSconsolida/logs
#PATH_RESPALDO=/export/home/bgw/GPRSconsolida/respaldos
#PATH_DESCARTADOS=/export/home/bgw/GPRSconsolida/descartados
#TIP_ARCHIVO=processed

PATH_RAIZ=$1
PATH_TMP=$2
PATH_SALIDA=$3
PATH_LOG=$4
PATH_RESPALDO=$5
TIP_ARCHIVO=$6  # valores del tipo processed y roamer
PATH_MEDIACION=$7
CHARGIN=$8 # ejemplo CGMX2

A_ENTRADA=${PATH_TMP}/AE${TIP_ARCHIVO}.${CHARGIN}
A_ORDENADO=${PATH_TMP}/AO${TIP_ARCHIVO}.${CHARGIN}
A_TEMPORAL=${PATH_TMP}/AT${TIP_ARCHIVO}.${CHARGIN}
A_SALIDA=${PATH_SALIDA}/AS${TIP_ARCHIVO}.${CHARGIN}
A_LOG=${PATH_LOG}/BITACORA_${TIP_ARCHIVO}_${FECHA}.log

#variables para archivos de salida
FEC_EJEC=`date +%Y%m%d%H%M%S`
VPRE_FILE=${PATH_SALIDA}/${TIP_ARCHIVO}_
#VPOS_FILE=_${FEC_EJEC}.cdr.CONS1
VNLF=7000


#roamer_05_20130404234438.cdr.CGMX2
#roamermx_MTYCGAPP5_20131203111838.cdr
#roamermx_DFNCGAPP4_20131203111503.cdr
#processed_11_20130920123101.cdr.CGMX8

cd ${PATH_RAIZ}

# Depura archivos temporales
rm -f ${A_ENTRADA}
rm -f ${A_ORDENADO}
rm -f ${A_SALIDA}
#rm -f ${A_LOG}

echo "............... Inicia proceso de consolidacion de archivos ${TIP_ARCHIVO} - ${CHARGIN} `date +%Y%m%d" "%T`"  >> ${A_LOG}

# movemos archivos a carpeta de trabajo
mv ${TIP_ARCHIVO}_*.${CHARGIN} ${PATH_TMP}
if [ $? -ne 0 ]
then 
echo "Fallo al mover archivos ${TIP_ARCHIVO} - ${CHARGIN} a path ${PATH_TMP}"  >> ${A_LOG}
exit 1
fi

cd ${PATH_TMP}

# Se obtiene fecha-hora del archivo mas reciente a procesar
if [ ${TIP_ARCHIVO} == "processed" ]
then 
{
VPOS_FILE=`ls -lt ${TIP_ARCHIVO}_*.${CHARGIN}|head -1|awk '{print substr($9,11,13)}'`
VMINUTO=`ls -lt ${TIP_ARCHIVO}_*.${CHARGIN}|head -1|awk '{print substr($9,24,1)}'`
}
else
{
VPOS_FILE=`ls -lt ${TIP_ARCHIVO}_*.${CHARGIN}|head -1|awk '{print substr($9,8,13)}'`
VMINUTO=`ls -lt ${TIP_ARCHIVO}_*.${CHARGIN}|head -1|awk '{print substr($9,21,1)}'`
}
fi

#A_DESCARTADOS=${PATH_DESCARTADOS}/AD${TIP_ARCHIVO}${VPOS_FILE}.txt

# concatenamos archivos
cat ${TIP_ARCHIVO}_*.${CHARGIN} > ${A_ENTRADA}
if [ $? -ne 0 ]
then 
echo "Fallo al concatenar archivos ${TIP_ARCHIVO} - ${CHARGIN}" >> ${A_LOG}
exit 1
fi

#############################################################################
#    CONSOLIDA GPRS


nawk -v tarchivo=${TIP_ARCHIVO} '{
recordtype=substr($0,1,3)
c1=substr($0,4,14)
imsi=substr($0,18,16)
otime=substr($0,34,10)
otimecomplete=substr($0,34,14)
#c=substr($0,44,4)
imei=substr($0,48,16)
c2=substr($0,64,2)
dn=substr($0,66,18)
c3=substr($0,84,74)
apn=substr($0,158,63)
c4=substr($0,221,114)
accessType=substr($0,335,1)
c5=substr($0,336,9)
airinterface=substr($0,345,1)
c6=substr($0,346,34)
sUplink=substr($0,380,12)
sDownlink=substr($0,392,12)
c7=substr($0,404,172)
gUplink=substr($0,576,12)
gDownlink=substr($0,588,12)
c8=substr($0,600,122)
tipog=3
if(recordtype=="018" && airinterface=="1")
{
tipog=1
}
if(recordtype=="018" && airinterface=="2")
{
tipog=0
}
if((recordtype=="019" || recordtype=="099") && accessType=="1")
{
tipog=2
}
if((recordtype=="019" || recordtype=="099") && accessType=="2")
{
tipog=1
}
if(((recordtype=="019" || recordtype=="099") && tarchivo=="processed") || (recordtype=="018" && tarchivo=="roamer"))
{
print dn"|"otime"|"imsi"|"imei"|"recordtype"|"apn"|"tipog"|"otimecomplete"|"c1"|"c2"|"c3"|"c4"|"accessType"|"c5"|"airinterface"|"c6"|"sUplink"|"sDownlink"|"c7"|"gUplink"|"gDownlink"|"c8
}
}' ${A_ENTRADA}|sort > ${A_ORDENADO}

if [ $? -ne 0 ]
then 
echo "Fallo al ordenar archivo del tipo ${TIP_ARCHIVO} - ${CHARGIN}"  >> ${A_LOG}
exit 1
fi

# Obtenemos numero de lineas de archivo ordenado
veof=`wc -l ${A_ORDENADO}|awk '{print $1}'`

echo "Lineas entrada ${A_ORDENADO}: ${veof}" >> ${A_LOG}

########################################     AGRUPA Y SUMARIZA VOLUMEN
INICIO=0

nawk -F"|" -v eof=${veof} -v nlf=${VNLF} -v pre_file=${VPRE_FILE} -v pos_file=${VPOS_FILE} -v extension=${CHARGIN} -v mediacion=${PATH_MEDIACION} -v minuto=${VMINUTO} '{
dn=$1
otime=$2
imsi=$3
imei=$4
recordtype=$5
apn=$6
tipog=$7
otimecomplete=$8
c1=$9
c2=$10
c3=$11
c4=$12
accesstype=$13
c5=$14
airinterface=$15
c6=$16
suplink=$17
sdownlink=$18
c7=$19
guplink=$20
gdownlink=$21
c8=$22


if(NR==1)
{
VDN=dn
VOTIME=otime
VIMSI=imsi
VIMEI=imei
VRECORDTYPE=recordtype
VAPN=apn
VTIPOG=tipog
VOTIMECOMPLETE=otimecomplete
VC1=c1
VC2=c2
VC3=c3
VC4=c4
VACCESSTYPE=accesstype
VC5=c5
VAIRINTERFACE=airinterface
VC6=c6
VC7=c7
VC8=c8
VSUPLINK=0
VSDOWNLINK=0
VGUPLINK=0
VGDOWNLINK=0

VSUPLINK+=suplink
VSDOWNLINK+=sdownlink
VGUPLINK+=guplink
VGDOWNLINK+=gdownlink
#sec1=0
#sec1+=0
minuto+=0
sec2=0
sec2+=0
lineas=0
lineas+=0

FILE_NAME=pre_file pos_file minuto "000.cdr." extension

}
else
{
if(dn==VDN && otime==VOTIME && imsi==VIMSI && imei==VIMEI && recordtype==VRECORDTYPE && apn==VAPN && tipog==VTIPOG)  #=========  ACUMULA VOLUMEN ===========
{
# Condicion para volumenes que rebasen los 12 digitos
if((VSUPLINK+suplink > 999999999999) || (VSDOWNLINK+sdownlink > 999999999999) || (VGUPLINK+guplink > 999999999999) || (VGDOWNLINK+gdownlink > 999999999999))
{
 printf "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%012s%012s%s%012s%012s%s\n", VRECORDTYPE,VC1,VIMSI,VOTIMECOMPLETE,VIMEI,VC2,VDN,VC3,VAPN,VC4,VACCESSTYPE,VC5,VAIRINTERFACE,VC6,VSUPLINK,VSDOWNLINK,VC7,VGUPLINK,VGDOWNLINK,VC8 >> FILE_NAME   
lineas+=1
VDN=dn
VOTIME=otime
VIMSI=imsi
VIMEI=imei
VRECORDTYPE=recordtype
VAPN=apn
VTIPOG=tipog
VOTIMECOMPLETE=otimecomplete
VC1=c1
VC2=c2
VC3=c3
VC4=c4
VACCESSTYPE=accesstype
VC5=c5
VAIRINTERFACE=airinterface
VC6=c6
VC7=c7
VC8=c8
VSUPLINK=0
VSDOWNLINK=0
VGUPLINK=0
VGDOWNLINK=0
}
VSUPLINK+=suplink
VSDOWNLINK+=sdownlink
VGUPLINK+=guplink
VGDOWNLINK+=gdownlink
}    #======================================== ACUMULA VOLUMEN FIN =================================================================================
else
{

#Controla cantidad de lineas por archivo
if(lineas>=nlf)
{
close(FILE_NAME)
sec2+=1
#sec1+=1
#if(sec1>99)
#{
#sec1=0
#sec1+=0
#sec2+=1
#}

if(sec2>999)
{
minuto+=1
sec2=0
sec2+=0
}

#if(sec1<10)
#{
#  s1="0"sec1
#}
#else {s1=sec1}

if(sec2<10) {s2="00"sec2}
else if(sec2<100) {s2="0"sec2}
else {s2=sec2}


lineas=0
lineas+=0
FILE_NAME=pre_file pos_file minuto s2 ".cdr." extension
}

if(length(VSUPLINK)>12) {VSUPLINK=substr(VSUPLINK,1,12)}
if(length(VSDOWNLINK)>12) {VSDOWNLINK=substr(VSDOWNLINK,1,12)}
if(length(VGUPLINK)>12) {VGUPLINK=substr(VGUPLINK,1,12)}
if(length(VGDOWNLINK)>12) {VGDOWNLINK=substr(VGDOWNLINK,1,12)}

 printf "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%012s%012s%s%012s%012s%s\n", VRECORDTYPE,VC1,VIMSI,VOTIMECOMPLETE,VIMEI,VC2,VDN,VC3,VAPN,VC4,VACCESSTYPE,VC5,VAIRINTERFACE,VC6,VSUPLINK,VSDOWNLINK,VC7,VGUPLINK,VGDOWNLINK,VC8 >> FILE_NAME   

lineas+=1

VDN=dn
VOTIME=otime
VIMSI=imsi
VIMEI=imei
VRECORDTYPE=recordtype
VAPN=apn
VTIPOG=tipog
VOTIMECOMPLETE=otimecomplete
VC1=c1
VC2=c2
VC3=c3
VC4=c4
VACCESSTYPE=accesstype
VC5=c5
VAIRINTERFACE=airinterface
VC6=c6
VC7=c7
VC8=c8
VSUPLINK=0
VSDOWNLINK=0
VGUPLINK=0
VGDOWNLINK=0
VSUPLINK+=suplink
VSDOWNLINK+=sdownlink
VGUPLINK+=guplink
VGDOWNLINK+=gdownlink

}     

} # fin del else principal

if(NR == eof) 
{ 
if(length(VSUPLINK)>12) {VSUPLINK=substr(VSUPLINK,1,12)}
if(length(VSDOWNLINK)>12) {VSDOWNLINK=substr(VSDOWNLINK,1,12)}
if(length(VGUPLINK)>12) {VGUPLINK=substr(VGUPLINK,1,12)}
if(length(VGDOWNLINK)>12) {VGDOWNLINK=substr(VGDOWNLINK,1,12)}
printf "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%012s%012s%s%012s%012s%s\n", VRECORDTYPE,VC1,VIMSI,VOTIMECOMPLETE,VIMEI,VC2,VDN,VC3,VAPN,VC4,VACCESSTYPE,VC5,VAIRINTERFACE,VC6,VSUPLINK,VSDOWNLINK,VC7,VGUPLINK,VGDOWNLINK,VC8 >> FILE_NAME
}

}' ${A_ORDENADO}



if [ $? -ne 0 ]
then 
echo "Fallo al generar archivos consolidados de ${TIP_ARCHIVO} - ${CHARGIN} "  >> ${A_LOG}
# se depuran archivos de salida generados por el proceso
cd ${PATH_SALIDA}
rm -f ${TIP_ARCHIVO}_*${VPOS_FILE}*.cdr.${CHARGIN}
exit 1
fi

# Obtenemos numero de lineas de archivo consolidado
OUT_ROWS=`cat ${PATH_SALIDA}/${TIP_ARCHIVO}_*${VPOS_FILE}*.cdr.${CHARGIN}|wc -l|awk '{print $1}'`

echo "Lineas consolidadas de: ${TIP_FILE} - ${CHARGIN}: ${OUT_ROWS}" >> ${A_LOG}



#     FIN CONSOLIDA
##############################################################################


cd ${PATH_TMP}

# se respaldan archivos consolidados
mv ${TIP_ARCHIVO}_*.${CHARGIN} ${PATH_RESPALDO}

if [ $? -ne 0 ]
then 
echo "Error al respaldar archivos ${TIP_ARCHIVO} - ${CHARGIN}, favor de liberar espacio en el filesystem ${PATH_RESPALDO} y ejecutar los siguientes comandos"  >> ${A_LOG}
echo "cd ${PATH_TMP}" >> ${A_LOG}
echo "mv ${TIP_ARCHIVO}_*.${CHARGIN} ${PATH_RESPALDO}" >> ${A_LOG}
# se depuran archivos de salida generados por el proceso
cd ${PATH_SALIDA}
rm -f ${TIP_ARCHIVO}_*${VPOS_FILE}*.cdr.${CHARGIN}
exit 1
fi

#Alimenta al Mediador con los archivos consolidados
cd ${PATH_SALIDA}
mv ${TIP_ARCHIVO}_*${VPOS_FILE}*.cdr.${CHARGIN} ${PATH_MEDIACION}



echo "======================== Finaliza ejecucion exitosa de archivos ${TIP_ARCHIVO} - ${CHARGIN} `date +%Y%m%d" "%T` ===================="   >> ${A_LOG}

exit 0



