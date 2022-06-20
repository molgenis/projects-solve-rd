import { createApp } from 'vue'
import { createRouter, createWebHashHistory } from 'vue-router'

import App from './App.vue'
import Home from './pages/Home.vue'
import GetStarted from './pages/GetStarted.vue'
import PatientTree from './pages/PatientTree.vue'

const routes = [
  { path: '/', component: Home },
  { path: '/get-started', component: GetStarted },
  { path: '/patient-tree', component: PatientTree }
]

const router = createRouter({
  history: createWebHashHistory(),
  routes
})

const app = createApp(App)
app.use(router)
app.mount('#app')