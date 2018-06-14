#!/bin/bash
OUTFILE=/tmp/qdone-test-child-kill-linux.out
rm $OUTFILE
_term() { 
  echo "terminated" > $OUTFILE
  exit 1
}
trap _term SIGTERM
for i in 1 2 3; do sleep 1; echo $i; echo $i >> $OUTFILE; done
