FROM node:18-alpine

WORKDIR /usr/src/app

# Copiar archivos de dependencias
COPY package*.json ./

# Instalar dependencias
RUN npm install --only=production --legacy-peer-deps

# Copiar el código de la aplicación (desde src/)
COPY src/ .

EXPOSE 8080

CMD [ "node", "index.js" ]