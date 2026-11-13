import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Khi phát triển, proxy tới Training Control độc lập ở localhost:8001.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5174,
    proxy: {
      "/api": "http://localhost:8001",
    },
  },
});
