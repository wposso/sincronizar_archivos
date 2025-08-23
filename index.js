const { Storage } = require('@google-cloud/storage');
const { GoogleAuth } = require('google-auth-library');
const fetch = require('node-fetch');
const express = require('express');

// Configuración
const BUCKET_NAME = process.env.BUCKET_NAME || "talenthub_central";
const ROOT_FOLDER_ID = process.env.ROOT_FOLDER_ID || "1PcnN9zwjl9w_b9y99zS6gKWMhwIVdqfD";
const PORT = process.env.PORT || 8080;

// Cliente de Google Cloud Storage
const storage = new Storage();
const app = express();

// Middleware básico
app.use(express.json());

// Variable global para almacenar el último tiempo de sync
let lastSyncTime = '2000-01-01T00:00:00.000Z';

/**
 * Ruta principal que Cloud Run health check requiere
 */
app.get('/', (req, res) => {
    res.status(200).send('✅ Servicio de sincronización Drive to GCS activo');
});

/**
 * Ruta para ejecutar la sincronización manualmente
 */
app.post('/sync', async (req, res) => {
    console.log("🔍 Iniciando sincronización incremental de Drive a GCS");

    try {
        // Autenticación automática en Google Cloud
        const auth = new GoogleAuth({
            scopes: [
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/cloud-platform'
            ]
        });

        const client = await auth.getClient();
        const token = (await client.getAccessToken()).token;

        // Obtener última fecha de ejecución
        lastSyncTime = await getLastSyncTime();
        const currentTime = new Date().toISOString();

        console.log("Buscando archivos modificados desde: " + lastSyncTime);

        const stats = await processFolderIncremental(ROOT_FOLDER_ID, "", token, lastSyncTime);

        // Actualizar marca de tiempo
        await setLastSyncTime(currentTime);

        console.log(`✅ Sincronización completada. 
Nuevos/Modificados: ${stats.ok} 
Fallidos: ${stats.fail} 
Carpetas: ${stats.folders}`);

        res.status(200).json({
            status: 'success',
            message: 'Sincronización completada',
            stats: stats
        });

    } catch (error) {
        console.error("❌ Error en ejecución:", error);
        res.status(500).json({
            status: 'error',
            message: error.message
        });
    }
});

/**
 * Ruta para sincronización completa
 */
app.post('/sync-full', async (req, res) => {
    console.log("🔄 Iniciando sincronización COMPLETA");

    // Resetear última sincronización
    lastSyncTime = '2000-01-01T00:00:00.000Z';

    // Ejecutar sincronización incremental que procesará todo
    try {
        const auth = new GoogleAuth({
            scopes: [
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/cloud-platform'
            ]
        });

        const client = await auth.getClient();
        const token = (await client.getAccessToken()).token;

        const stats = await processFolderIncremental(ROOT_FOLDER_ID, "", token, lastSyncTime);

        res.status(200).json({
            status: 'success',
            message: 'Sincronización completa completada',
            stats: stats
        });

    } catch (error) {
        console.error("❌ Error en sincronización completa:", error);
        res.status(500).json({
            status: 'error',
            message: error.message
        });
    }
});

// ✅ SERVIR LA APP EN EL PUERTO OBLIGATORIO
app.listen(PORT, () => {
    console.log(`🚀 Servidor ejecutándose en puerto ${PORT}`);
    console.log(`📌 Health check disponible en: http://localhost:${PORT}/`);
    console.log(`🔄 Sincronización incremental: POST http://localhost:${PORT}/sync`);
    console.log(`🔄 Sincronización completa: POST http://localhost:${PORT}/sync-full`);
});

/**
 * Procesa carpetas recursivamente solo con archivos modificados
 */
async function processFolderIncremental(folderId, prefix, token, modifiedSince) {
    let ok = 0, fail = 0, folders = 0;

    // Solo archivos modificados después de lastRun
    const q = `'${folderId}' in parents and trashed = false and modifiedTime > '${modifiedSince}'`;
    const items = await listFilesInFolderREST(folderId, token, q);

    if (items.length === 0) {
        // Verificar si la carpeta está vacía y crear placeholder si es necesario
        if (await isFolderEmpty(folderId, token)) {
            try {
                const placeholderName = prefix + "__placeholder";
                await uploadBlobToGCS(BUCKET_NAME, placeholderName, Buffer.from(""), "text/plain", token);
                console.log("📂 Carpeta vacía → " + placeholderName);
                ok++;
            } catch (err) {
                console.log("❌ ERROR creando placeholder: " + prefix + " :: " + err.message);
                fail++;
            }
        }
        return { ok, fail, folders };
    }

    console.log("🔄 Procesando " + items.length + " items en: " + prefix);

    for (const item of items) {
        if (item.mimeType === "application/vnd.google-apps.folder") {
            folders++;
            const subStats = await processFolderIncremental(item.id, prefix + item.name + "/", token, modifiedSince);
            ok += subStats.ok;
            fail += subStats.fail;
            folders += subStats.folders;
        } else {
            try {
                const blob = await downloadDriveFileREST(item.id, item.mimeType, token);
                const objectName = prefix + buildObjectNameFromFile(item.name, blob);
                await uploadBlobToGCS(BUCKET_NAME, objectName, blob, item.mimeType, token);
                console.log("📤 SUBIDO → " + objectName);
                ok++;
            } catch (err) {
                console.log("❌ ERROR → " + item.name + " :: " + err.message);
                fail++;
            }
        }
    }

    return { ok, fail, folders };
}

/**
 * Verifica si una carpeta está completamente vacía
 */
async function isFolderEmpty(folderId, token) {
    const q = `'${folderId}' in parents and trashed = false`;
    const items = await listFilesInFolderREST(folderId, token, q);
    return items.length === 0;
}

/**
 * Lista archivos en carpeta con query personalizable
 */
async function listFilesInFolderREST(folderId, token, customQuery) {
    const files = [];
    let pageToken = null;
    const q = customQuery || `'${folderId}' in parents and trashed = false`;

    do {
        const url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(q)}` +
            `&fields=nextPageToken,files(id,name,mimeType,modifiedTime)&pageSize=1000` +
            (pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : "");

        const response = await fetch(url, {
            headers: { Authorization: "Bearer " + token },
        });

        if (!response.ok) {
            throw new Error(`Drive list error ${response.status} :: ${await response.text()}`);
        }

        const data = await response.json();
        if (data.files && data.files.length) {
            files.push(...data.files);
        }
        pageToken = data.nextPageToken || null;

    } while (pageToken);

    return files;
}

/**
 * Descarga archivo de Drive
 */
async function downloadDriveFileREST(fileId, mimeType, token) {
    let url;
    if (mimeType && mimeType.indexOf("application/vnd.google-apps") === 0) {
        url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}/export?mimeType=${encodeURIComponent("application/pdf")}`;
    } else {
        url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}?alt=media`;
    }

    const response = await fetch(url, {
        headers: { Authorization: "Bearer " + token },
    });

    if (!response.ok) {
        throw new Error(`Drive download error ${response.status} :: ${await response.text()}`);
    }

    const buffer = await response.buffer();
    return buffer;
}

/**
 * Construye nombre de objeto para GCS
 */
function buildObjectNameFromFile(name, blob) {
    // Para este ejemplo, asumimos que el blob es un Buffer
    // En la práctica, podrías necesitar verificar el tipo MIME de otra manera
    if (name && !name.toLowerCase().endsWith('.pdf')) {
        return name + ".pdf";
    }
    return name;
}

/**
 * Sube blob a Google Cloud Storage
 */
async function uploadBlobToGCS(bucket, objectName, blob, contentType, token) {
    const file = storage.bucket(bucket).file(objectName);

    await file.save(blob, {
        metadata: {
            contentType: contentType || 'application/octet-stream',
        },
    });

    console.log(`✅ Archivo subido: ${objectName}`);
}

// Agrega este endpoint ESPECÍFICO para mensajes de Pub/Sub
app.post('/sync/webhook', async (req, res) => {
    try {
        console.log('📩 Mensaje recibido de Pub/Sub');

        // Los mensajes de Pub/Sub vienen en formato especial
        if (req.body.message && req.body.message.data) {
            const messageData = Buffer.from(req.body.message.data, 'base64').toString();
            console.log('Contenido del mensaje:', messageData);

            // Aquí procesarías el mensaje para saber qué archivo sincronizar
            const auth = new GoogleAuth({ scopes: ['https://www.googleapis.com/auth/drive'] });
            const client = await auth.getClient();
            const token = (await client.getAccessToken()).token;

            // Extraer el ID del archivo que cambió (depende del formato del mensaje)
            // Esto es un ejemplo - necesitarías adaptarlo al formato real
            const fileId = extractFileIdFromMessage(messageData);

            if (fileId) {
                await syncSingleFile(fileId, token);
                console.log('✅ Archivo sincronizado desde Pub/Sub');
            }
        }

        res.status(200).send('✅ Procesado');
    } catch (error) {
        console.error('❌ Error procesando mensaje Pub/Sub:', error);
        res.status(500).send('Error');
    }
});

// Endpoint para webhooks de Drive (TIEMPO REAL)
app.post('/sync/webhook', async (req, res) => {
    console.log('📩 Notificación de Drive recibida en tiempo real!');
    console.log('Headers:', req.headers);

    // Responder inmediatamente a Drive (importante)
    res.status(200).send('✅ Notificación recibida');

    // Procesar en segundo plano
    setTimeout(async () => {
        try {
            const auth = new GoogleAuth({
                keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS || 'drive-key.json',
                scopes: ['https://www.googleapis.com/auth/drive']
            });

            const client = await auth.getClient();
            const token = (await client.getAccessToken()).token;

            // Extraer información del cambio
            const resourceId = req.headers['x-goog-resource-id'];
            const resourceState = req.headers['x-goog-resource-state'];

            console.log('🔄 Procesando cambio en tiempo real:');
            console.log('   Resource ID:', resourceId);
            console.log('   Resource State:', resourceState);

            if (resourceState === 'change') {
                // Aquí va tu lógica para sincronizar el archivo específico
                console.log('📤 Sincronizando archivo cambiado...');
                // await syncSingleFile(resourceId, token);
            }

        } catch (error) {
            console.error('❌ Error procesando notificación:', error);
        }
    }, 1000);
});

// Función de ejemplo para extraer fileId (debes adaptarla)
function extractFileIdFromMessage(message) {
    try {
        const data = JSON.parse(message);
        return data.id || data.fileId || null;
    } catch (e) {
        console.log('Mensaje no es JSON, buscando patrones...');
        // Aquí lógica para extraer el ID de diferentes formatos
        return null;
    }
}

/**
 * Obtiene último tiempo de sincronización (simplificado)
 */
async function getLastSyncTime() {
    // En producción, aquí leerías de Firestore o Cloud Storage
    return lastSyncTime;
}

/**
 * Guarda último tiempo de sincronización (simplificado)
 */
async function setLastSyncTime(time) {
    // En producción, aquí guardarías en Firestore o Cloud Storage
    lastSyncTime = time;
}

// Exportar para testing
module.exports = { app, processFolderIncremental };