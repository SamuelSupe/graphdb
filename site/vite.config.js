import { defineConfig } from "vite";

export default defineConfig({
  base: "/graphdb/",
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
