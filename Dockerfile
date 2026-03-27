# ── Stage 1: Build ──────────────────────────────────────────────
FROM node:22-slim AS builder

# better-sqlite3 requires native compilation
RUN apt-get update && apt-get install -y python3 make g++ && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src/ ./src/
RUN npm run build

# ── Stage 2: Production deps (with build tools) ────────────────
FROM node:22-slim AS deps

RUN apt-get update && apt-get install -y python3 make g++ && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# ── Stage 3: Runtime ───────────────────────────────────────────
FROM node:22-slim AS runtime

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

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
