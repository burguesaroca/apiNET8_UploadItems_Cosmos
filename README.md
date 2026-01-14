# Upload Items to Cosmos DB

Aplicación de consola .NET 8 para cargar múltiples documentos JSON a Azure Cosmos DB de forma automática.

## Configuración Inicial

### 1. Configurar Cosmos DB

Edita el archivo `appsettings.json` con tus credenciales de Cosmos DB:

```json
{
  "CosmosDb": {
    "Endpoint": "https://TU-CUENTA-COSMOS.documents.azure.com:443/",
    "Key": "TU-CLAVE-PRIMARIA",
    "DatabaseName": "requestdb",
    "ContainerName": "connections"
  }
}
```

**Importante**: Asegúrate de que:
- La base de datos `requestdb` existe en tu cuenta de Cosmos DB
- El contenedor `connections` existe con `clientId` como partition key

### 2. Preparar tus Conexiones

El archivo `connections.json` contiene 10 conexiones de ejemplo. Puedes:
- Modificar los datos de ejemplo con tus conexiones reales
- Agregar más conexiones al array JSON
- Mantener el formato del JSON

Cada conexión debe tener esta estructura:
```json
{
  "id": "guid-unico",
  "clientId": "CL001",
  "clientName": "NOMBRE_CLIENTE",
  "servidor": "servidor\\instancia",
  "puerto": "1433",
  "user": "usuario",
  "password": "password-encriptado",
  "repository": "nombre-bd",
  "adapter": "SqlServerSP"
}
```

## Uso

### 1. Restaurar paquetes e instalar dependencias

```bash
dotnet restore
```

### 2. Ejecutar la aplicación

```bash
dotnet run
```

La aplicación:
1. Lee todas las conexiones del archivo `connections.json`
2. Se conecta a Cosmos DB usando las credenciales de `appsettings.json`
3. Carga cada conexión al contenedor `connections`
4. Muestra el progreso y resultado de cada operación
5. Al final muestra un resumen con éxitos y errores

## Características

- ✅ Carga automática de múltiples documentos
- ✅ Usa `UpsertItemAsync` para insertar o actualizar documentos existentes
- ✅ Manejo de errores por documento (si uno falla, continúa con los demás)
- ✅ Muestra el consumo de RUs (Request Units) por operación
- ✅ Resumen final con estadísticas de la carga

## Partition Key

La aplicación usa `clientId` como partition key. Si tu contenedor usa una partition key diferente, modifica esta línea en `Program.cs`:

```csharp
var response = await container.UpsertItemAsync(
    connection,
    new PartitionKey(connection.ClientId)  // Cambiar si es diferente
);
```

## Notas

- El campo `id` debe ser único para cada documento
- Si un documento con el mismo `id` existe, será actualizado (upsert)
- Los passwords están en formato encriptado/base64 como en tu ejemplo
- La aplicación copia automáticamente los archivos JSON al directorio de salida

## Docker

El proyecto incluye un `Dockerfile` multi-stage para compilar y ejecutar la aplicación.

- Construir la imagen:

```powershell
docker build -t apinet8-uploaditems-cosmos:latest .
```

- Ejecutar (por defecto mapea el puerto interno 5008):

```powershell
docker run --rm -p 5008:5008 apinet8-uploaditems-cosmos:latest
```

- Sobrescribir el puerto con variable de entorno:

```powershell
# Usando ASPNETCORE_URLS
docker run --rm -p 5010:5010 -e ASPNETCORE_URLS=http://+:5010 apinet8-uploaditems-cosmos:latest

# O usando WEB_PORT (fallback del host si no se establece ASPNETCORE_URLS)
docker run --rm -p 5011:5011 -e WEB_PORT=5011 apinet8-uploaditems-cosmos:latest
```

- Health check:

```powershell
curl http://localhost:5008/health
```

- Pasar `appsettings.json` o secretos por volumen o variables de entorno:

```powershell
docker run --rm -p 5008:5008 -v ${PWD}/appsettings.json:/app/appsettings.json apinet8-uploaditems-cosmos:latest
```

Nota: evita incluir credenciales sensibles en la imagen. Prefiere montarlas en tiempo de ejecución o usar un secrets manager.
