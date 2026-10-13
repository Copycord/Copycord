// src/copycord/admin/frontend/vite.config.ts
import { defineConfig } from "vite";
import { resolve } from "node:path";

export default defineConfig({
  root: ".",
  base: "/static/",
  build: {
    outDir: "dist",
    assetsDir: "assets",
    manifest: false,   // no manifest needed
    rollupOptions: {
      input: { main: resolve(__dirname, "src/main.js") },
      output: {
        entryFileNames: "assets/main.js",
        chunkFileNames: "assets/[name].js",
        assetFileNames: (info) => {
          if (info.name && info.name.endsWith(".css")) return "assets/main.css";
          return "assets/[name][extname]";
        },
      },
    },
  },
  server: { port: 5173 },
});
