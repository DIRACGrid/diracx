#!/bin/bash
echo "Starting slow job on $(hostname) at $(date)"
for i in $(seq 1 120); do
    echo "Heartbeat $i at $(date)"
    sleep 1
done
echo "Slow job finished at $(date)"
