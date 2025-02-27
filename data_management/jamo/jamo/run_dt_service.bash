:
WAIT=120

module load python
export PYTHONPATH=/global/cfs/cdirs/seqfs/code_and_ui/cori_prod/jamo_source/jgi-sdm-common/lib/python


while true
do
    jamo_status=`curl -k -s https://jamo.jgi.doe.gov/api/tape/diskusage | grep files`
    if [ "$jamo_status" == "" ] ; then
        echo `date` jamo down
    else
       machine=`uname -n`
       export PYTHONHTTPSVERIFY=0
       if [ "$machine" == "dtn03.nersc.gov" ] ; then
           echo "starting ingest,delete,purge,put,copy,md5,tar"
           python3 /global/cfs/cdirs/seqfs/code_and_ui/cori_prod/jamo_source/jgi-jamo/dt_service.py -t 5 -f nersc,dna_w,hsi_1,hsi_2,compute -k ingest,delete,purge,put,copy,md5,tar -r 0 https://jamo.jgi.doe.gov
       fi
       if [ "$machine" == "dtn04.nersc.gov" ] ; then
           echo "starting prep,pull"
           python3 /global/cfs/cdirs/seqfs/code_and_ui/cori_prod/jamo_source/jgi-jamo/dt_service.py -t 10 -f dna_w,hsi_1,compute -k prep,pull -r 0 https://jamo.jgi.doe.gov
       fi
    fi

    sleep $WAIT

    if [ -e "EXIT" ] ; then
        exit
    fi
done
