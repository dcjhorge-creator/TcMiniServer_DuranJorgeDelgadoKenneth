##Mini Server

JorgeDuranChinchilla C02679
KennethFabricioDelgadoCardenas C22540

Intrucciones para correr la tc3 producer-consumer

Abrir dos terminales, en una de ellas:


make clean

make server_procon
make load_client

luego correr el server con ./bin/server_procon 8080 4


luego en la otra terminal correr los clientes: ./bin/load_client 127.0.0.1 8080 4 150
o con time ./bin/load_client 127.0.0.1 8080 4 150

Ctrl + c para terminar
