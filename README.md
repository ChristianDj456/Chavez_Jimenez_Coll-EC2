# Chavez_Jimenez_Coll-EC2
Entrega Generador.py
Estructura del Computador 2
Agregada la paralelización
Tiempo de ejecucion en pruebas entre 2000 y 3000 segundos
Modo de ejecucion:  docker run --rm -it --name mpicont -v Directorio:/app --workdir=/app augustosalazar/un_mpi_network:1 mpirun -n 4 -oversubscribe --allow-run-as-root python generador.py -d ./input -jrt -jm -jcrt -h ht.txt -fi 01-01-16 -ff 30-09-16
