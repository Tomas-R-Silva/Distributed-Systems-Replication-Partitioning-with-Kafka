@echo off

IF "%~1"=="" (
    echo usage: start-kafka.bat localhost or start-kafka.bat kafka
    exit /b 1
)

docker network ls | findstr "sd-net" >nul 2>&1
IF ERRORLEVEL 1 (
    docker network create --driver=bridge --subnet=172.20.0.0/16 sdnet
)

docker pull smduarte/sd2324-kafka

echo Launching Kafka Server: %1

docker rm -f kafka

docker run -h %1 ^
           --name=kafka ^
           --network=sdnet ^
           --rm -t -p 9092:9092 -p 2181:2181 smduarte/sd2324-kafka