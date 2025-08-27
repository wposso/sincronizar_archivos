FROM node:18-alpine

WORKDIR /usr/src/app

# Copiar archivos de dependencias
COPY package*.json ./

# Instalar dependencias con legacy-peer-deps
RUN npm install --only=production --legacy-peer-deps

# Copiar TODO el código (incluyendo la carpeta src)
COPY . .

EXPOSE 8080

# Ejecutar desde la carpeta src
CMD [ "node", "src/index.js" ]