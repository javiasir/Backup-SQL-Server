from auth import *
import pyodbc 
 
# ENCRYPT defaults to yes starting in ODBC Driver 18. It's good to always specify ENCRYPT=yes on the client side to avoid MITM attacks.
# cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';ENCRYPT=yes;UID='+username+';PWD='+password)

try:
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password+';autocommit=True')
    cursor = cnxn.cursor()
    select = cursor.execute("SELECT name from sys.databases;")
    
    nom_db = select.fetchall()
    bases_datos = []
    for x in nom_db: # Los nombres de las bases de datos, sin tener que formatear
        #print(x[0])
        if x[0] == 'master' or x[0] == 'model' or x[0] == 'msdb' or x[0] == 'tempdb': # Salta las BD del sistema
            continue
        else:
            bases_datos.append(x[0])
    
    for n in bases_datos:    
        backup = cursor.execute(f"""
            BACKUP DATABASE {n} 
                TO DISK = {path.format(n)}
                    WITH NOFORMAT,
                    NOINIT,
                    NAME = '{n}',
                    SKIP,
                    NOREWIND,
                    NOUNLOAD,
                    STATS = 10;
            """)
        while cursor.nextset(): # Para "consumir" los mensajes de progreso emitidos por el BACKUP
            pass
    
    cnxn.close()
    
    print("Conectado con exito") 
except Exception as ex:
    print("Error al intentar conectar")
    print(ex)


#print(pyodbc.drivers()) # Comprobar que drivers utiliza
