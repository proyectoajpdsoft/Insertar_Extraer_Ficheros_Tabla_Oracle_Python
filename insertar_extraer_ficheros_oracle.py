import oracledb

# Conexión al servidor de base de datos Oracle
cadenaConexion = oracledb.makedsn("localhost", "1521", service_name="ORCLCDB")
conexionOracle = oracledb.connect(user="SYSTEM", password="contraseña", dsn=cadenaConexion)

# Inserta un registro en la tabla de Oracle con un campo de 
# tipo BLOB para insertar un fichero
def insertarFactura(pCliente, pImporte, pFactura):
    try:
        # Leer el fichero en modo binario
        with open(pFactura, "rb") as file:
            facturaBLOB = file.read()
        # Insertar el registro en la tabla, con el fichero adjunto
        with conexionOracle.cursor() as cursor:
            sql = "insert into factura (cliente, importe, factura) VALUES (:cliente, :importe, :factura)"
            cursor.execute(sql, cliente=pCliente, importe=pImporte, factura=facturaBLOB)
            conexionOracle.commit()
            print("Factura insertada correctamente en la tabla de Oracle.")

    except Exception as e:
        print(f"Error al insertar la factura: {e}")
        
# Estráe un fichero de la tabla de Oracle del campo tipo BLOB
def extraerFactura(pCodigo, pRutaDestino):
    try:
        with conexionOracle.cursor() as cursor:
            # Obtener el fichero BLOB de la tabla
            sql = "select factura from factura where codigo = :codigo"
            cursor.execute(sql, codigo=pCodigo)
            # Obtenemos el primer registro de la 
            # consulta SQL (solo debe obtener uno al ser el código clave primaria)
            resultado = cursor.fetchone()
            if resultado:
                facturaLOB = resultado[0] # Objeto LOB de la tabla Oracle
                facturaBYTES = facturaLOB.read() # Convertimos LOB en bytes                
                # Guardamos el fichero en la ruta y nombre indicados
                with open(pRutaDestino, "wb") as file:
                    file.write(facturaBYTES)
                print(f"Factura extraída correctamente en {pRutaDestino}.")                
            else:
                print(f"No se ha encontrado la factura con el código {pCodigo}.")

    except Exception as e:
        print(f"Error al extraer la factura: {e}")
        
def mostrarFacturas():
    try:
        with conexionOracle.cursor() as cursor:
            # Consulta para obtener todos los registros de la tabla factura
            # Como ejemplo, el campo "factura" al ser BLOB no se puede 
            # mostrar directamente, lo que hacemos es mostrar su tamaño
            sql = "select codigo, fecha, cliente, DBMS_LOB.GETLENGTH(factura) as Factura_Bytes from factura"
            cursor.execute(sql)
            resultados = cursor.fetchall()

            if resultados:
                print("Listado de facturas:")
                print("-" * 50)
                print("{:<5} {:<20} {:<30} {:<10}".format("Código", "Fecha", "Cliente", "Factura (bytes)"))
                print("-" * 50)
                for factura in resultados:
                    codigo, fecha, cliente, tamano = factura
                    print(f"{codigo:<5} {fecha.strftime('%Y-%m-%d %H:%M:%S'):<20} {cliente:<30} {tamano:<10}")
            else:
                print("No hay facturas en la tabla.")

    except Exception as e:
        print(f"Error al mostrar las facturas: {e}")        
        
# ** Probar el código Python **

# Insertar varias facturas con fichero adjunto
insertarFactura(pCliente="ProyectoA 1", pImporte=1010.55, pFactura=r"D:\ProyectoA_Factura\Factura1.pdf")
insertarFactura(pCliente="ProyectoA 2", pImporte=21545.95, pFactura=r"D:\ProyectoA_Factura\Factura2.pdf")
insertarFactura(pCliente="ProyectoA 3", pImporte=800, pFactura=r"D:\ProyectoA_Factura\Factura145.pdf")
insertarFactura(pCliente="ProyectoA 2", pImporte=256.05, pFactura=r"D:\ProyectoA_Factura\Factura205.pdf")
insertarFactura(pCliente="ProyectoA 1", pImporte=10.8, pFactura=r"D:\ProyectoA_Factura\Factura343.pdf")
insertarFactura(pCliente="ProyectoA 3", pImporte=258.89, pFactura=r"D:\ProyectoA_Factura\Factura95.pdf")

# Mostrar los registros de la tabla factura
mostrarFacturas()

#Extraer varios ficheros de factura de varias facturas
extraerFactura(1, r"D:\ProyectoA_Factura\Fatura_Extraida1.pdf")
extraerFactura(2, r"D:\ProyectoA_Factura\Fatura_Extraida2.pdf")
extraerFactura(5, r"D:\ProyectoA_Factura\Fatura_Extraida5.pdf")