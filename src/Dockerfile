FROM node:18-alpine

WORKDIR /app

# Copiar package.json e instalar dependencias
COPY package*.json ./
RUN npm install --only=production

# Copiar el código de la aplicación
COPY . .

# Exponer el puerto
EXPOSE 8080

# Ejecutar la aplicación
CMD ["node", "index.js"]