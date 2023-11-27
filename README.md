# Chavez_Jimenez_Coll-EC2
Entrega Generador.py y generadorp.py
Estructura del Computador 2
Agregada la paralelización
Tiempo de ejecucion en pruebas: 700 segundos secuencial, y 200 segundos paralelo
Modo de ejecucion:  docker run --rm -it --name mpicont -v Directorio:/app --workdir=/app augustosalazar/un_mpi_network:1 mpirun -n 8 -oversubscribe --allow-run-as-root python generadorp.py -d ./input -jrt -jm -jcrt -h ht.txt -fi 01-01-16 -ff 30-09-16
