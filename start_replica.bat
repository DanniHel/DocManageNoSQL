@echo off
echo Iniciando Nodo 1...
start "MongoDB Nodo 1" cmd /k "mongod --replSet rs0 --dbpath C:\data\rs\node1 --logpath C:\data\rs\log\node1.log --port 27017 --bind_ip localhost"

echo Iniciando Nodo 2...
start "MongoDB Nodo 2" cmd /k "mongod --replSet rs0 --dbpath C:\data\rs\node2 --logpath C:\data\rs\log\node2.log --port 27018 --bind_ip localhost"

echo Iniciando Nodo 3...
start "MongoDB Nodo 3" cmd /k "mongod --replSet rs0 --dbpath C:\data\rs\node3 --logpath C:\data\rs\log\node3.log --port 27019 --bind_ip localhost"

echo Todos los nodos iniciados. Usa mongosh --port 27017 para conectar e inicializar.
pause