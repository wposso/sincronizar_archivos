const { Storage } = require('@google-cloud/storage');
const { GoogleAuth } = require('google-auth-library');
const { google } = require('googleapis');
const fetch = require('node-fetch');
const express = require('express');
const { Firestore } = require('@google-cloud/firestore');
const { PubSub } = require('@google-cloud/pubsub');

// Configuración
const BUCKET_NAME = process.env.BUCKET_NAME || "talenthub_central";
const ROOT_FOLDER_ID = process.env.ROOT_FOLDER_ID || "1PcnN9zwjl9w_b9y99zS6gKWMhwIVdqfD";
const PORT = process.env.PORT || 8080;
const WEBHOOK_URL = process.env.WEBHOOK_URL;
const SYNC_TOPIC = process.env.SYNC_TOPIC || "drive-sync-topic";

// Clientes
const storage = new Storage();
const firestore = new Firestore();
const pubsub = new PubSub();
const app = express();

// Middleware
app.use(express.json());

// Colección para almacenar estado de sincronización
const SYNC_COLLECTION = 'drive_sync_state';
const WEBHOOK_COLLECTION = 'drive_webhooks';

// Para evitar procesamiento duplicado de notificaciones
const processedChanges = new Set();
const CHANGE_TTL = 300000; // 5 minutos

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
 * Renueva automáticamente los webhooks antes de que expiren
 */
async function renewWebhooks() {
    try {
        const snapshot = await firestore.collection(WEBHOOK_COLLECTION).get();

        for (const doc of snapshot.docs) {
            const webhookData = doc.data();
            const expirationTime = parseInt(webhookData.expiration);

            // Renovar si expira en menos de 4 horas
            if (expirationTime - Date.now() < 4 * 60 * 60 * 1000) {
                console.log('🔄 Renovando webhook que expira pronto:', webhookData.id);

                const auth = new GoogleAuth({
                    scopes: ['https://www.googleapis.com/auth/drive']
                });

                const client = await auth.getClient();
                const drive = google.drive({ version: 'v3', auth: client });

                // Obtener token de página inicial
                const startPageToken = await drive.changes.getStartPageToken();

                // Renovar webhook
                await drive.channels.stop({
                    requestBody: {
                        id: webhookData.id,
                        resourceId: webhookData.resourceId
                    }
                });

                const newWebhook = await drive.changes.watch({
                    pageToken: startPageToken.data.startPageToken,
                    requestBody: {
                        id: webhookData.id,
                        type: 'web_hook',
                        address: `${WEBHOOK_URL}/sync/webhook`,
                        expiration: (Date.now() + 86400000).toString(), // 24 horas
                    }
                });

                // Actualizar en Firestore
                await firestore.collection(WEBHOOK_COLLECTION).doc(webhookData.id).set({
                    id: newWebhook.data.id,
                    resourceId: newWebhook.data.resourceId,
                    expiration: newWebhook.data.expiration,
                    address: newWebhook.data.address,
                    updatedAt: new Date().toISOString()
                });

                console.log('✅ Webhook renovado:', newWebhook.data.id);
            }
        }
    } catch (error) {
        console.error('❌ Error renovando webhooks:', error.message);
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

        // Generar ID único para el webhook
        const webhookId = 'drive-to-gcs-sync-webhook-' + Date.now();

        // Configurar webhook
        const response = await drive.changes.watch({
            pageToken: startPageToken.data.startPageToken,
            requestBody: {
                id: webhookId,
                type: 'web_hook',
                address: `${WEBHOOK_URL}/sync/webhook`,
                expiration: (Date.now() + 86400000).toString(), // 24 horas
            }
        });

        // Guardar información del webhook en Firestore para renovación automática
        await firestore.collection(WEBHOOK_COLLECTION).doc(webhookId).set({
            id: response.data.id,
            resourceId: response.data.resourceId,
            expiration: response.data.expiration,
            address: response.data.address,
            createdAt: new Date().toISOString()
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
 * Procesa cambios en tiempo real con manejo de duplicados
 */
async function processRealTimeChange(changeId, fileId, resourceState) {
    // Evitar procesamiento duplicado
    if (processedChanges.has(changeId)) {
        console.log('⏭️  Cambio ya procesado:', changeId);
        return;
    }

    // Agregar a procesados con TTL
    processedChanges.add(changeId);
    setTimeout(() => processedChanges.delete(changeId), CHANGE_TTL);

    try {
        const auth = new GoogleAuth({
            scopes: ['https://www.googleapis.com/auth/drive']
        });

        const client = await auth.getClient();
        const token = (await client.getAccessToken()).token;

        // Obtener información del archivo modificado
        const driveResponse = await fetch(
            `https://www.googleapis.com/drive/v3/files/${fileId}?fields=id,name,mimeType,modifiedTime,parents,trashed`,
            { headers: { Authorization: `Bearer ${token}` } }
        );

        if (!driveResponse.ok) {
            throw new Error(`Error obteniendo información del archivo: ${driveResponse.status}`);
        }

        const file = await driveResponse.json();

        // Si el archivo está en papelera, eliminarlo de GCS
        if (file.trashed) {
            try {
                // Intentar eliminar de GCS
                const fileName = file.name;
                await storage.bucket(BUCKET_NAME).file(fileName).delete();
                console.log(`🗑️  Archivo eliminado de GCS: ${fileName}`);
            } catch (deleteError) {
                if (deleteError.code === 404) {
                    console.log(`⚠️  Archivo no encontrado en GCS para eliminar: ${file.name}`);
                } else {
                    throw deleteError;
                }
            }
            return;
        }

        console.log(`📤 Sincronizando: ${file.name} (${resourceState})`);

        // Si es una carpeta, procesar recursivamente
        if (file.mimeType === "application/vnd.google-apps.folder") {
            await processFolderIncremental(file.id, file.name + "/", token, new Date(0).toISOString());
        } else {
            // Descargar y subir el archivo
            const blob = await downloadDriveFileREST(file.id, file.mimeType, token);
            await uploadBlobToGCS(BUCKET_NAME, file.name, blob, file.mimeType);
            console.log(`✅ Sincronizado en tiempo real: ${file.name}`);
        }

    } catch (error) {
        console.error('❌ Error procesando cambio en tiempo real:', error);

        // Reintentar después de un delay usando Pub/Sub para mejor escalabilidad
        await pubsub.topic(SYNC_TOPIC).publishMessage({
            data: Buffer.from(JSON.stringify({
                changeId: changeId,
                fileId: fileId,
                resourceState: resourceState,
                retryCount: 1
            }))
        });
    }
}

/**
 * Obtiene último page token de sincronización desde Firestore
 */
async function getLastSyncPageToken() {
    try {
        const doc = await firestore.collection(SYNC_COLLECTION).doc('page_token').get();
        if (doc.exists) {
            return doc.data().token;
        }

        // Si no existe, obtener uno nuevo
        const auth = new GoogleAuth({
            scopes: ['https://www.googleapis.com/auth/drive']
        });
        const client = await auth.getClient();
        const drive = google.drive({ version: 'v3', auth: client });
        const startPageToken = await drive.changes.getStartPageToken();

        await setLastSyncPageToken(startPageToken.data.startPageToken);
        return startPageToken.data.startPageToken;
    } catch (error) {
        console.error('Error obteniendo page token:', error);
        throw error;
    }
}

/**
 * Guarda último page token de sincronización en Firestore
 */
async function setLastSyncPageToken(token) {
    try {
        await firestore.collection(SYNC_COLLECTION).doc('page_token').set({
            token: token,
            updatedAt: new Date().toISOString()
        });
    } catch (error) {
        console.error('Error guardando page token:', error);
        throw error;
    }
}

/**
 * Webhook mejorado para notificaciones en tiempo real de Drive
 */
app.post('/sync/webhook', async (req, res) => {
    console.log('📩 Notificación de Drive recibida!');

    // Verificar que es una notificación válida de Drive
    const resourceId = req.headers['x-goog-resource-id'];
    const resourceState = req.headers['x-goog-resource-state'];
    const resourceUri = req.headers['x-goog-resource-uri'];
    const channelId = req.headers['x-goog-channel-id'];

    if (!resourceId || !resourceState) {
        console.log('⚠️  Notificación inválida, faltan headers necesarios');
        return res.status(400).send('Notificación inválida');
    }

    // Generar ID único para este cambio
    const changeId = `${resourceId}-${Date.now()}`;

    // Responder inmediatamente (Drive requiere respuesta rápida)
    res.status(200).send('✅ Notificación recibida');

    // Procesar en segundo plano
    setTimeout(async () => {
        try {
            console.log(`🔄 Procesando cambio: ${resourceState} para resource: ${resourceId}`);

            // Para cambios, necesitamos obtener los archivos modificados
            if (resourceState === 'change' || resourceState === 'update' || resourceState === 'add') {
                const auth = new GoogleAuth({
                    scopes: ['https://www.googleapis.com/auth/drive']
                });

                const client = await auth.getClient();
                const drive = google.drive({ version: 'v3', auth: client });

                // Obtener el page token actual
                const pageToken = await getLastSyncPageToken();

                // Obtener los cambios recientes
                const changes = await drive.changes.list({
                    pageToken: pageToken,
                    pageSize: 10
                });

                // Procesar cada cambio
                for (const change of changes.data.changes) {
                    if (change.fileId) {
                        await processRealTimeChange(
                            `${change.fileId}-${Date.now()}`,
                            change.fileId,
                            resourceState
                        );
                    }
                }

                // Actualizar el page token
                if (changes.data.newStartPageToken) {
                    await setLastSyncPageToken(changes.data.newStartPageToken);
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

// Agrega este endpoint para los health checks de Google
app.post('/sync/scheduled', (req, res) => {
    console.log('✅ Health check recibido de Google');
    res.status(200).json({
        status: 'ok',
        message: 'Service is running',
        timestamp: new Date().toISOString()
    });
});

// También agrega un endpoint GET para health checks
app.get('/sync/scheduled', (req, res) => {
    console.log('✅ Health check GET recibido');
    res.status(200).json({
        status: 'ok',
        message: 'Service is healthy',
        timestamp: new Date().toISOString()
    });
});

// Endpoint raíz para health checks
app.get('/', (req, res) => {
    res.json({
        service: 'Drive to GCS Sync',
        status: 'running',
        timestamp: new Date().toISOString(),
        endpoints: {
            health: '/',
            manual_sync: 'POST /sync',
            webhook: 'POST /sync/webhook',
            scheduled: '/sync/scheduled'
        }
    });
});

// Iniciar servidor
app.listen(PORT, async () => {
    console.log(`🚀 Servidor ejecutándose en puerto ${PORT}`);
    console.log(`📌 Health check disponible en: http://localhost:${PORT}/`);
    console.log(`🔄 Sincronización manual: POST http://localhost:${PORT}/sync`);
    console.log(`🌐 Webhook: POST http://localhost:${PORT}/sync/webhook`);

    await setupDriveWebhook();
    startDrivePolling();

    // Iniciar renovación automática de webhooks cada hora
    setInterval(renewWebhooks, 60 * 60 * 1000);
});

module.exports = { app };