#! /bin/bash

for i in $(ls *.dot); do
  NAME=$(basename $i .dot)
  echo "Ploting ($NAME)"
  dot  -Tpng  -o $NAME.png $NAME.dot
done
