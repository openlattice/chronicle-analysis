FOLDER=$(pwd)

docker run \
  -v $FOLDER:$FOLDER \
  openlattice/chroniclepy:v1.2 \
  all \
  $FOLDER/resources/rawdata \
  $FOLDER/resources/preprocessed \
  $FOLDER/resources/subsetted \
  $FOLDER/resources/output

docker run \
  -v $FOLDER:$FOLDER \
  --entrypoint python \
  openlattice/chroniclepy:v1.2 \
  $FOLDER/check_output.py --directory $FOLDER





