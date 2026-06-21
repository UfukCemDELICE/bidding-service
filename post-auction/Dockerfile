FROM node:22-alpine
WORKDIR /app

# Install OS deps for pg and other native modules
RUN apk add --no-cache python3 make g++

COPY package*.json ./
RUN npm ci --omit=dev

COPY . .

EXPOSE 3000

# Run schema init then start the service
CMD ["sh", "-c", "node src/db/init.js && node src/app.js"]
