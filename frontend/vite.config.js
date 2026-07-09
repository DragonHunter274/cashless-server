import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

// https://vite.dev/config/
export default defineConfig({
  plugins: [svelte()],
  base: './',
  build: {
    outDir: '../static',
    emptyOutDir: false,
  },
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
      '/auth': 'http://localhost:8080',
      '/makePurchase': 'http://localhost:8080',
      '/confirmPurchase': 'http://localhost:8080',
      '/makeCashPurchase': 'http://localhost:8080',
      '/getBalance': 'http://localhost:8080',
      '/getTransactions': 'http://localhost:8080',
      '/getVouchers': 'http://localhost:8080',
      '/getPrivileges': 'http://localhost:8080',
      '/topUp': 'http://localhost:8080',
      '/createUser': 'http://localhost:8080',
      '/createVoucher': 'http://localhost:8080',
      '/createPrivilege': 'http://localhost:8080',
      '/getStats': 'http://localhost:8080',
      '/getUsers': 'http://localhost:8080',
      '/getAPIKeys': 'http://localhost:8080',
      '/createAPIKey': 'http://localhost:8080',
      '/deleteAPIKey': 'http://localhost:8080',
      '/getProductMap': 'http://localhost:8080',
      '/createProductMapping': 'http://localhost:8080',
      '/deleteProductMapping': 'http://localhost:8080',
      '/deleteVoucher': 'http://localhost:8080',
      '/deletePrivilege': 'http://localhost:8080',
      '/uploadFirmware': 'http://localhost:8080',
      '/getFirmwareList': 'http://localhost:8080',
      '/activateFirmware': 'http://localhost:8080',
      '/deleteFirmware': 'http://localhost:8080',
      '/firmware': 'http://localhost:8080',
    }
  }
})
