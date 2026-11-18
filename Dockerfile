# Debian, not Alpine: the Temporal worker ships a native (Rust) core built for glibc only.
FROM node:24-bookworm-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY proto ./proto
COPY db/saga.sql ./db/saga.sql
COPY src ./src
# Workflow code bundled once here, so the saga worker restarts in about a second.
RUN node src/saga/bundle.ts
COPY public ./public
USER node
