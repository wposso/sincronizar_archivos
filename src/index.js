const { Storage } = require('@google-cloud/storage');
const { GoogleAuth } = require('google-auth-library');
const { google } = require('googleapis');
const fetch = require('node-fetch');
const express = require('express');
const { Firestore } = require('@google-cloud/firestore');

// Configuración
const BUCKET_NAME = process.env.BUCKET_NAME || "talenthub_central";
const ROOT_FOLDER_ID = process.env.ROOT_FOLDER_ID || "1PcnN9zwjl9w_b9y99zS6gKWMhwIVdqfD";
const PORT = process.env.PORT || 8080;
const WEBHOOK_URL = process.env.WEBHOOK_URL;

// Clientes
const storage = new Storage();
const firestore = new Firestore();
const app = express();

// Middleware
app.use(express.json());

// Colección para almacenar estado de sincronización
const SYNC_COLLECTION = 'drive_sync_state';

/**
 * Obtiene último tiempo de sincronización desde Firestore
 */
async function getLastSyncTime() {
    try {
        const doc = await firestore.collection(SYNC_COLLECTION).doc('last_sync').get();
        return doc.exists ? doc.data().timestamp : '2000-01-01T00:00:00.000Z';
    } catch (error) {
        console.error('Error obteniendo lastSyncTime:', error);
        return '2000-01-01T00:00:00.000Z';
    }
}

/**
 * Guarda último tiempo de sincronización en Firestore
 */
async function setLastSyncTime(timestamp) {
    try {
        await firestore.collection(SYNC_COLLECTION).doc('last_sync').set({
            timestamp: timestamp,
            updatedAt: new Date().toISOString()
        });
    } catch (error) {
        console.error('Error guardando lastSyncTime:', error);
    }
}

/**
 * Configuración inicial del webhook de Drive
 */
async function setupDriveWebhook() {
    try {
        if (!WEBHOOK_URL) {
            console.log('⚠️  WEBHOOK_URL no configurada. Solo funcionará polling');
            return;
        }

        const auth = new GoogleAuth({
            scopes: ['https://www.googleapis.com/auth/drive']
        });

        const client = await auth.getClient();
        const drive = google.drive({ version: 'v3', auth: client });

        // Obtener token de página inicial
        const startPageToken = await drive.changes.getStartPageToken();
        console.log('🔑 Token de página inicial:', startPageToken.data.startPageToken);

        // Configurar webhook
        const response = await drive.changes.watch({
            pageToken: startPageToken.data.startPageToken,
            requestBody: {
                id: 'drive-to-gcs-sync-webhook-' + Date.now(),
                type: 'web_hook',
                address: `${WEBHOOK_URL}/sync/webhook`,
                expiration: (Date.now() + 86400000).toString(), // 24 horas
            }
        });

        console.log('✅ Webhook de Drive configurado exitosamente!');
        console.log('📋 Resource ID:', response.data.resourceId);
        console.log('🌐 Drive notificará a:', WEBHOOK_URL);
        console.log('⏰ Expira:', new Date(parseInt(response.data.expiration)).toLocaleString());

    } catch (error) {
        console.error('❌ Error configurando webhook:', error.message);
        if (error.response?.data) {
            console.error('Detalles del error:', error.response.data);
        }
    }
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
            `&fields=nextPageToken,files(id,name,mimeType,modifiedTime,parents)&pageSize=1000` +
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
 * Sube blob a Google Cloud Storage
 */
async function uploadBlobToGCS(bucket, objectName, blob, contentType) {
    const file = storage.bucket(bucket).file(objectName);
    await file.save(blob, {
        metadata: {
            contentType: contentType || 'application/octet-stream',
        },
    });
    console.log(`✅ Archivo subido a GCS: ${objectName}`);
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
 * Procesa carpetas recursivamente solo con archivos modificados
 */
async function processFolderIncremental(folderId, prefix, token, modifiedSince) {
    let ok = 0, fail = 0, folders = 0;

    const q = `'${folderId}' in parents and trashed = false and modifiedTime > '${modifiedSince}'`;
    const items = await listFilesInFolderREST(folderId, token, q);

    if (items.length === 0) {
        if (await isFolderEmpty(folderId, token)) {
            try {
                const placeholderName = prefix + "__placeholder";
                await uploadBlobToGCS(BUCKET_NAME, placeholderName, Buffer.from(""), "text/plain");
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
                const objectName = prefix + item.name;
                await uploadBlobToGCS(BUCKET_NAME, objectName, blob, item.mimeType);
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
 * Webhook para notificaciones en tiempo real de Drive
 */
app.post('/sync/webhook', async (req, res) => {
    console.log('📩 Notificación de Drive recibida!');

    // Responder inmediatamente (Drive requiere respuesta rápida)
    res.status(200).send('✅ Notificación recibida');

    // Procesar en segundo plano
    setTimeout(async () => {
        try {
            const auth = new GoogleAuth({
                scopes: ['https://www.googleapis.com/auth/drive']
            });

            const client = await auth.getClient();
            const token = (await client.getAccessToken()).token;

            const resourceId = req.headers['x-goog-resource-id'];
            const resourceState = req.headers['x-goog-resource-state'];

            console.log(`🔄 Procesando cambio: ${resourceState} para resource: ${resourceId}`);

            if (resourceState === 'change' || resourceState === 'update' || resourceState === 'add') {
                // Obtener información del archivo modificado
                const driveResponse = await fetch(
                    `https://www.googleapis.com/drive/v3/files/${resourceId}?fields=id,name,mimeType,modifiedTime,parents`,
                    { headers: { Authorization: `Bearer ${token}` } }
                );

                if (driveResponse.ok) {
                    const file = await driveResponse.json();

                    console.log(`📤 Sincronizando: ${file.name}`);

                    const blob = await downloadDriveFileREST(file.id, file.mimeType, token);
                    await uploadBlobToGCS(BUCKET_NAME, file.name, blob, file.mimeType);

                    console.log(`✅ Sincronizado en tiempo real: ${file.name}`);
                }
            }

        } catch (error) {
            console.error('❌ Error procesando webhook:', error);
        }
    }, 1000);
});

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
    console.log("🔍 Iniciando sincronización manual de Drive a GCS");

    try {
        const auth = new GoogleAuth({
            scopes: ['https://www.googleapis.com/auth/drive']
        });

        const client = await auth.getClient();
        const token = (await client.getAccessToken()).token;

        const lastSyncTime = await getLastSyncTime();
        const currentTime = new Date().toISOString();

        console.log("Buscando archivos modificados desde: " + lastSyncTime);

        const stats = await processFolderIncremental(ROOT_FOLDER_ID, "", token, lastSyncTime);
        await setLastSyncTime(currentTime);

        console.log(`✅ Sincronización manual completada. 
Archivos: ${stats.ok} 
Fallidos: ${stats.fail} 
Carpetas: ${stats.folders}`);

        res.status(200).json({
            status: 'success',
            message: 'Sincronización completada',
            stats: stats
        });

    } catch (error) {
        console.error("❌ Error en sincronización manual:", error);
        res.status(500).json({
            status: 'error',
            message: error.message
        });
    }
});

/**
 * Polling automático cada 30 segundos
 */
const POLLING_INTERVAL = 30000; // 30 segundos

async function startDrivePolling() {
    console.log(`🔄 Iniciando polling automático cada ${POLLING_INTERVAL / 1000} segundos...`);

    setInterval(async () => {
        try {
            console.log('⏰ Polling: Buscando cambios en Drive...');

            const auth = new GoogleAuth({
                scopes: ['https://www.googleapis.com/auth/drive']
            });

            const client = await auth.getClient();
            const token = (await client.getAccessToken()).token;

            const lastRun = await getLastSyncTime();
            const fiveMinutesAgo = new Date(Date.now() - 5 * 60000).toISOString();
            const modifiedSince = lastRun < fiveMinutesAgo ? fiveMinutesAgo : lastRun;

            const stats = await processFolderIncremental(ROOT_FOLDER_ID, "", token, modifiedSince);

            if (stats.ok > 0) {
                await setLastSyncTime(new Date().toISOString());
                console.log(`✅ Polling: ${stats.ok} archivos sincronizados`);
            }

        } catch (error) {
            console.error('❌ Error en polling automático:', error.message);
        }
    }, POLLING_INTERVAL);
}

// Iniciar servidor
app.listen(PORT, async () => {
    console.log(`🚀 Servidor ejecutándose en puerto ${PORT}`);
    console.log(`📌 Health check disponible en: http://localhost:${PORT}/`);
    console.log(`🔄 Sincronización manual: POST http://localhost:${PORT}/sync`);
    console.log(`🌐 Webhook: POST http://localhost:${PORT}/sync/webhook`);

    await setupDriveWebhook();
    startDrivePolling();
});

module.exports = { app };