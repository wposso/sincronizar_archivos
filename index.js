const { Storage } = require('@google-cloud/storage');
const { GoogleAuth } = require('google-auth-library');
const fetch = require('node-fetch');

// Configuración (puedes usar variables de entorno)
const BUCKET_NAME = process.env.BUCKET_NAME || "talenthub_central";
const ROOT_FOLDER_ID = process.env.ROOT_FOLDER_ID || "1PcnN9zwjl9w_b9y99zS6gKWMhwIVdqfD";

// Cliente de Google Cloud Storage
const storage = new Storage();

// Variable global para almacenar el último tiempo de sync
let lastSyncTime = '2000-01-01T00:00:00.000Z';

/**
 * Función principal que se ejecuta en Cloud Run/Functions
 */
exports.syncDriveToGCS = async (req, res) => {
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

        // Obtener última fecha de ejecución (aquí usamos una variable, en producción usarías Firestore/Cloud Storage)
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
};

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

/**
 * Función para sincronización completa
 */
exports.syncDriveToGCSFull = async (req, res) => {
    console.log("🔄 Iniciando sincronización COMPLETA");

    // Resetear última sincronización
    lastSyncTime = '2000-01-01T00:00:00.000Z';

    // Ejecutar incremental que procesará todo
    await this.syncDriveToGCS(req, res);
};

// Para ejecución local (opcional)
if (require.main === module) {
    exports.syncDriveToGCS({}, { status: (code) => ({ send: (msg) => console.log(msg) }) });
}