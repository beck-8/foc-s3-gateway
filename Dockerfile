# ── Stage 1: Build ──────────────────────────────────────────────
FROM node:22-alpine AS builder

# better-sqlite3 requires native compilation
RUN apk add --no-cache python3 make g++

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src/ ./src/
RUN npm run build

# ── Stage 2: Production deps (with build tools) ────────────────
FROM node:22-alpine AS deps

RUN apk add --no-cache python3 make g++

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# ── Stage 3: Runtime ───────────────────────────────────────────
FROM node:22-alpine AS runtime

RUN apk add --no-cache curl

WORKDIR /app
COPY package.json ./
COPY --from=deps /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist

# Persistent data directory (SQLite DB + staged files)
ENV DB_PATH=/data/metadata.db
VOLUME /data

# S3 (8333) + WebDAV (8334)
EXPOSE 8333 8334

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -sf http://localhost:8333/_/status || exit 1

ENTRYPOINT ["node", "dist/cli.js", "serve"]
