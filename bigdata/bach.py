# Importar las bibliotecas necesarias
from pyspark.sql import SparkSession, functions as F

# Crear una sesión de Spark
spark = SparkSession.builder.appName('tarea3').getOrCreate()

# Ruta del archivo en HDFS
data_path = 'hdfs://localhost:9000/tarea3/rows.csv'

# Cargar datos desde archivo CSV con configuración de encabezado y detección automática de esquema
data = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(data_path)

# Mostrar la estructura de los datos cargados
data.printSchema()

# Vista preliminar de los primeros registros
data.show(5)

# Remover registros duplicados
clean_data = data.dropDuplicates()

# Remover registros con valores nulos en cualquier columna
clean_data = clean_data.na.drop()

# Mostrar registros después de la limpieza
print("Datos después de eliminar duplicados y valores nulos:")
clean_data.show(5)

# Realizar un análisis estadístico básico
print("Estadísticas descriptivas del conjunto de datos limpio:")
clean_data.describe().show()

# Guardar el conjunto de datos limpio en formato CSV en el Escritorio y sobrescribir si el archivo existe
output_path = 'file:///home/vboxuser/Escritorio/cleaned_data_output.csv'
clean_data.write.mode('overwrite').format('csv').option('header', 'true').save(output_path)
